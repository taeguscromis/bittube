var express = require('express');
var router = express.Router();
var clone = require('clone');

/* GET home page. */
router.get('/', function(req, res) {
    res.redirect('/airtime');
});

/* GET DB chart. */
router.get('/airtime', function(req, res, next) {

  var db = req.db;
  var viewerscoll = db.get('viewers');
  var creatorscoll = db.get('creators');
  var alltimescoll = db.get('systemStats');
  var paymentscoll = db.get('overviewPayments');

  var allTimeUsers = 0;
  var allTimeViewers = 0;
  var allTimeChannels = 0;

  var airtimeChartTemplate = require('../reports/airtime.json');
  var alltimeChartTemplate = require('../reports/alltime.json');
  var earningChartTemplate = require('../reports/earning.json');
  var airTimeChartData = clone(airtimeChartTemplate);
  var allTimeChartData = clone(alltimeChartTemplate);
  var earningChartData = clone(earningChartTemplate);

  alltimescoll.find({},{},function(e,alltimedata)
  {
    if (alltimedata.length > 0)
    {

      allTimeUsers = alltimedata[alltimedata.length - 1].numusers;
      allTimeViewers = alltimedata[alltimedata.length - 1].numviewers;
      allTimeChannels = alltimedata[alltimedata.length - 1].numchannels;
    }

    for(var i = 0; i < alltimedata.length; i++)
    {
      var currDate = new Date(alltimedata[i].date);
      currDate.setDate(currDate.getDate() + 1);
      allTimeChartData.xAxis[0].data.push([currDate.getFullYear(), currDate.getMonth() + 1, currDate.getDate()].join('/'));
      allTimeChartData.xAxis[1].data.push([currDate.getFullYear(), currDate.getMonth() + 1, currDate.getDate()].join('/'));
      allTimeChartData.series[0].data.push(alltimedata[i].numviewers);
      allTimeChartData.series[1].data.push(alltimedata[i].numusers);
      allTimeChartData.series[2].data.push(alltimedata[i].numchannels);
    }

    creatorscoll.find({},{},function(e,creators)
    {

      for(var i = 0; i < creators.length; i++)
      {
        var currDate = new Date(creators[i].date);
        currDate.setDate(currDate.getDate() + 1);
        airTimeChartData.xAxis[0].data.push([currDate.getFullYear(), currDate.getMonth() + 1, currDate.getDate()].join('/'));
        earningChartData.xAxis[0].data.push([currDate.getFullYear(), currDate.getMonth() + 1, currDate.getDate()].join('/'));
        airTimeChartData.series[0].data.push(creators[i].count);
        earningChartData.series[0].data.push(creators[i].earnedPerDay);
      }

      viewerscoll.find({},{},function(e,viewers)
      {
        for(var i = 0; i < viewers.length; i++)
        {
          airTimeChartData.series[1].data.push(viewers[i].count);
          earningChartData.series[1].data.push(viewers[i].earnedPerDay);
        }

        paymentscoll.find({},{},function(e,payments)
        {
          for(var i = 0; i < payments.length; i++)
          {
            airTimeChartData.series[2].data.push(payments[i].count);
          }
  
          res.render('index',
          {
            title: 'BitTube airtime',
            airTimeChartData: JSON.stringify(airTimeChartData),
            allTimeChartData: JSON.stringify(allTimeChartData),
            earningChartData: JSON.stringify(earningChartData),
            alltime: {
              viewers: allTimeViewers,
              users: allTimeUsers,
              channels: allTimeChannels,
            }
          });
        });
      });
    });
  });
});

module.exports = router;