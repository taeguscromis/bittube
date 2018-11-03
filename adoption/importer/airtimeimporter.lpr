program airtimeimporter;

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}{$IFDEF UseCThreads}
  cthreads,
  {$ENDIF}{$ENDIF}
  Classes, SysUtils, CustApp, DateUtils, IniFiles, fphttpclient,

  // indy
  IdBaseComponent, IdComponent, IdHTTP, httpsend, ssl_openssl,

  // JSON
  fpjson, jsonparser,

  // mongo
  Mongo, MongoDB, MongoCollection, BSON, BSONTypes;

type

  { TAirTimeImporter }

  TAirTimeImporter = class(TCustomApplication)
  private
    procedure AddRecordToMongoDB(const collection: string; const aRecord: IBSONObject);
    procedure GetDataFromAirtime(const SearchType: string; const CurrentDate: TDateTime);
    procedure GetAllTimeDataFromAirtime(const SearchType: string; const CurrentDate: TDateTime);
  protected
    procedure DoRun; override;
  public
    constructor Create(TheOwner: TComponent); override;
    destructor Destroy; override;
    procedure WriteHelp; virtual;
  end;

{ TAirTimeImporter }

procedure TAirTimeImporter.AddRecordToMongoDB(const collection: string; const aRecord: IBSONObject);
var
  mongo: TMongo;
  db: TMongoDB;
  coll: TMongoCollection;
begin
  mongo := TMongo.Create;
  try
    mongo.Connect;
    db := mongo.getDB('bittube');
    coll := db.GetCollection(collection);
    coll.Insert(aRecord);
  finally
    mongo.Free;
  end;
end;

procedure TAirTimeImporter.GetAllTimeDataFromAirtime(const SearchType: string; const CurrentDate: TDateTime);
var
  HTTP: TidHTTP;
  ToDate: TDateTime;
  AResult: TMemoryStream;
  WorkDate: TDateTime;
  SingleRec: IBSONObject;
  JSONData: TJSONData;
  JSONParser: TJSONParser;
  Parameters: TStringList;
begin
  HTTP := TidHTTP.Create(nil);
  try
    WorkDate := CurrentDate;
    ToDate := Now;

    Parameters := TStringList.Create;
    try
      Parameters.StrictDelimiter := True;
      Parameters.Delimiter := '&';

      AResult := TMemoryStream.Create;
      try
        while DateOf(WorkDate) < DateOf(ToDate) do
        begin
          Parameters.Values['type'] := SearchType;
          HTTP.Request.ContentType := 'application/x-www-form-urlencoded';
          HTTP.Post('https://airtime.bit.tube/getStats', Parameters, AResult);
          AResult.Position := 0;

          JSONParser := TJSONParser.Create(AResult);
          try
            JSONData := JSONParser.Parse;
            try
              SingleRec := TBSONObject.Create;
              SingleRec.Put('date', WorkDate);
              SingleRec.Put('allestimated', JSONData.FindPath('stats[0].allestimated').AsFloat);
              SingleRec.Put('allcalculated', JSONData.FindPath('stats[0].allcalculated').AsFloat);
              SingleRec.Put('allrejected', JSONData.FindPath('stats[0].allrejected').AsFloat);
              SingleRec.Put('numviewers', JSONData.FindPath('stats[0].numviewers').AsInt64);
              SingleRec.Put('numusers', JSONData.FindPath('stats[0].numusers').AsInt64);
              SingleRec.Put('numchannels', JSONData.FindPath('stats[0].numchannels').AsInt64);
              AddRecordToMongoDB(SearchType, SingleRec);

              WorkDate := IncDay(WorkDate);
              Parameters.Clear;
              AResult.Clear;
            finally
              JSONData.Free;
            end;
          finally
            JSONParser.Free;
          end;
        end;
      finally
        AResult.Free;
      end;
    finally
      Parameters.Free;
    end;
  finally
    HTTP.Free;
  end;
end;

procedure TAirTimeImporter.GetDataFromAirtime(const SearchType: string; const CurrentDate: TDateTime);
var
  I: Integer;
  LogMsg: string;
  ToDate: TDateTime;
  AResult: TMemoryStream;
  LastByte: Byte;
  NumTries: Integer;
  Position: Integer;
  WorkDate: TDateTime;
  SingleRec: IBSONObject;
  JSONData: TJSONData;
  JSONArray: TJSONArray;
  JSONParser: TJSONParser;
  Parameters: TStringList;
  JSONStream: TMemoryStream;
  NoErrorResponse: Boolean;

  AirTime: Double;
  EarnedCoins: Double;
  SumEarnedPerDay: Double;
begin
  WorkDate := CurrentDate;
  ToDate := Now;

  Parameters := TStringList.Create;
  try
    Parameters.StrictDelimiter := True;
    Parameters.Delimiter := '&';

    AResult := TMemoryStream.Create;
    JSONStream := TMemoryStream.Create;
    try
      while DateOf(WorkDate) < DateOf(ToDate) do
      begin
        try
          NoErrorResponse := False;
          NumTries := 0;

          Parameters.Values['type'] := SearchType;
          Parameters.Values['pageNumber'] :=  '0';
          Parameters.Values['pageSize'] :=  '1000000';
          Parameters.Values['dateStart'] :=  FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz', WorkDate);
          Parameters.Values['dateEnd'] := FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz', IncDay(WorkDate));

          while (NoErrorResponse = False) and (NumTries < 5) do
          begin
            if HttpPostURL('https://airtime.bit.tube/getStats', Parameters.DelimitedText, AResult) then
            begin
              NoErrorResponse := True;
              AResult.Position := 0;
              LastByte := 0;

              while (AResult.Position < AResult.Size) and (LastByte <> Ord('{')) do
                LastByte := AResult.ReadByte;
              if AResult.Size > 0 then
               AResult.Position := AResult.Position - 1;

              if AResult.Position > 0 then
              begin
                AResult.SaveToFile(ExtractFilePath(ParamStr(0)) + 'AResult.json');

                NoErrorResponse := False;
                LogMsg := 'type: %s, date: %s, corrupted response, retrying %d!';
                WriteLn(Format(LogMsg, [SearchType, Parameters.Values['dateStart'], NumTries + 1]));
              end;

              JSONStream.Clear;
              JSONStream.CopyFrom(AResult, AResult.Size - AResult.Position);
              JSONStream.Position := 0;

              AResult.SaveToFile(ExtractFilePath(ParamStr(0)) + 'JSONStream.json');
            end
            else
            begin
              LogMsg := 'type: %s, date: %s, HTTP POST Failed!';
              WriteLn(Format(LogMsg, [SearchType, Parameters.Values['dateStart']]));
            end;

            Inc(NumTries);
          end;

          JSONParser := TJSONParser.Create(JSONStream);
          try
            JSONData := JSONParser.Parse;
            try
              SumEarnedPerDay := 0;

              // get the data count
              JSONArray := TJSONArray(JSONData.FindPath('stats'));

              if SameText(SearchType, 'creators') or SameText(SearchType, 'viewers') then
              begin
                for I := 0 to JSONArray.Count - 1 do
                begin
                  AirTime := JSONArray.Items[I].FindPath('airtime').AsFloat / 24 / 60 / 60;
                  if SameText(SearchType, 'creators') then
                    EarnedCoins := JSONArray.Items[I].FindPath('sum_creator_reward').AsFloat / 100000000
                  else if SameText(SearchType, 'viewers') then
                    EarnedCoins := JSONArray.Items[I].FindPath('sum_viewer_reward').AsFloat / 100000000;
                  SumEarnedPerDay := SumEarnedPerDay + (EarnedCoins / AirTime);
                end;
              end;

              SingleRec := TBSONObject.Create;
              SingleRec.Put('date', WorkDate);
              SingleRec.Put('count', JSONArray.Count);
              if SameText(SearchType, 'creators') or SameText(SearchType, 'viewers') then
              begin
                case JSONArray.Count > 0 of
                  true: SingleRec.Put('earnedPerDay', SumEarnedPerDay / JSONArray.Count);
                  false: SingleRec.Put('earnedPerDay', 0);
                end;
              end;

              WriteLn(Format('type: %s, date: %s', [SearchType, Parameters.Values['dateStart']]));
              AddRecordToMongoDB(SearchType, SingleRec);

              WorkDate := IncDay(WorkDate);
              Parameters.Clear;
              AResult.Clear;
            finally
              FreeAndNil(JSONData);
            end;
          finally
            FreeAndNil(JSONParser);
          end;
        except
          on E: Exception do
          begin
            WriteLn(Format('Error working on %s: %s', [SearchType, E.Message]));
          end;
        end;
      end;
    finally
      JSONStream.Free;
      AResult.Free;
    end;
  finally
    Parameters.Free;
  end;
end;

procedure TAirTimeImporter.DoRun;
var
  ErrorMsg: String;
  CurrentDate: TDateTime;
  LastDateName: string;
  LastDateFile: TIniFile;
begin
  // quick check parameters
  ErrorMsg:=CheckOptions('h', 'help');
  if ErrorMsg<>'' then begin
    ShowException(Exception.Create(ErrorMsg));
    Terminate;
    Exit;
  end;

  // parse parameters
  if HasOption('h', 'help') then begin
    WriteHelp;
    Terminate;
    Exit;
  end;

  LastDateName := ExtractFilePath(ParamStr(0)) + 'settings.ini';
  CurrentDate := EncodeDateTime(2018,8,1,0,0,0,0);
  if FileExists(LastDateName) then
  begin
    LastDateFile := TIniFile.Create(LastDateName);
    try
       CurrentDate := LastDateFile.ReadDate('Settings', 'LastDate', EncodeDateTime(2018,08,01,0,0,0,0));
    finally
      LastDateFile.Free;
    end;
  end;

  GetDataFromAirtime('viewers', CurrentDate);
  GetDataFromAirtime('creators', CurrentDate);
  GetDataFromAirtime('overviewPayments', CurrentDate);
  //GetAllTimeDataFromAirtime('systemStats', CurrentDate);

  LastDateFile := TIniFile.Create(LastDateName);
  try
     LastDateFile.WriteDate('Settings', 'LastDate', Now);
  finally
    LastDateFile.Free;
  end;

  // stop program loop
  Terminate;
end;

constructor TAirTimeImporter.Create(TheOwner: TComponent);
begin
  inherited Create(TheOwner);
  StopOnException:=True;
end;

destructor TAirTimeImporter.Destroy;
begin
  inherited Destroy;
end;

procedure TAirTimeImporter.WriteHelp;
begin
  { add your help code here }
  writeln('Usage: ', ExeName, ' -h');
end;

var
  Application: TAirTimeImporter;
begin
  Application:=TAirTimeImporter.Create(nil);
  Application.Title:='AirTimeImporter';
  Application.Run;
  Application.Free;
end.

