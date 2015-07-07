function Compare(a,b){   // Function for sorting the data based on time
if (a[0] < b[0]) return -1;
if (a[0] > b[0]) return 1;
return 0;
}
// Changing the Date into epoch time and saving the data in a array of array
var data1 = new Array();
var data2 = new Array();
var data3 = new Array();
var data4 = new Array();
var t = new Array();
for (i =0; i< time.length; i++){
    t[i] = time[i].trim()
    t[i] = t[i].split(" ")
    var qwe = t[i][0].split(".")
    var wer = t[i][1].split(":")
    console.log(qwe)
    console.log(wer)
    t[i] = moment.utc(time[i], "DD.MM.YYYY HH:mm").unix()*1000;
    time[i] = t[i]
}
for (i =0; i< time.length; i++){
    data1[i] = new Array();
    data1[i].push(time[i],y1[i])
}
data1 = data1.sort(Compare);

for (i =0; i< req_js.length; i++){
    data4[i] = new Object();
    data4[i].name = req_js[i]
    data4[i].y = reqc_js[i]
}





for (i =0; i< time.length; i++){
    data2[i] = new Array();
    data2[i].push(time[i],y2[i])
    }
data2 = data2.sort(Compare);


for (i =0; i< time.length; i++){
    data3[i] = new Array();
    data3[i].push(time[i],y3[i])
    }
data3 = data3.sort(Compare);

$(function () {
    $('#chart2').highcharts({
        chart: {
            type: 'spline'
        },
        title: {
            text: 'Trend of Response Time'
        },
        rangeSelector: {
                selected: 1
         },
        xAxis: {
            type: 'datetime',
            dateTimeLabelFormats: { // don't display the dummy year
            },
            title: {
                text: 'Timestamp'
            }
        },
        yAxis: {
            title: {
                text: 'Response Time (sec)'
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '<a href="">{point.x:%e-%b %H:%M},  {point.y:.2f} sec</a>',

        },

        plotOptions: {
            spline: {
            lineWidth: 1.5,
                states: {
                    hover: {
                        lineWidth: 2
                    }
                },
                marker: {
                    enabled: false
                }
            }
        },

        series: [{
            name: 'Response-time  vs  Time',
            // Define the data points. All series have a dummy year
            // of 1970/71 in order to be compared on the same x axis. Note
            // that in JavaScript, months start at 0 for January, 1 for February etc.
            data: data1
        }]

    });
});

$(function () {
    $('#chart1').highcharts({
        chart: {
            type: 'spline'
        },
        title: {
            text: 'Trend of Total Visits'
        },
        rangeSelector: {
                selected: 1
         },
        xAxis: {
            type: 'datetime',
            dateTimeLabelFormats: { // don't display the dummy year
            },
            title: {
                text: 'Timestamp'
            }
        },
        yAxis: {
            title: {
                text: 'Total Visits'
            },
            min: 0
        },
        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +'hits<br/><a target="_blank" href="http://localhost:5000/getinfo?key='+Highcharts.dateFormat('%d.%m.%Y %H:%M', this.x)+'"/>Click here to view data</a>' ;
        }
        },

        plotOptions: {
            spline: {
            lineWidth: 1.5,
                states: {
                    hover: {
                        lineWidth: 2
                    }
                },
                marker: {
                    enabled: false
                }
            }
        },

        series: [{
            name: 'Total Visits  vs  Time',
            // Define the data points. All series have a dummy year
            // of 1970/71 in order to be compared on the same x axis. Note
            // that in JavaScript, months start at 0 for January, 1 for February etc.
            data: data2
        },
        {
            name: 'Unique Visits  vs  Time',
            // Define the data points. All series have a dummy year
            // of 1970/71 in order to be compared on the same x axis. Note
            // that in JavaScript, months start at 0 for January, 1 for February etc.
            data: data3
        }]

    });
});


// Pie chart for the request type Data
$('#piechart2').highcharts({
        chart: {
            plotBackgroundColor: null,
            plotBorderWidth: null,
            plotShadow: false,
            type: 'pie'
        },
        title: {
            text: 'Request Types'
        },
        tooltip: {
            pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
        },
        plotOptions: {
            pie: {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                    style: {
                        color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || 'black'
                    }
                }
            }
        },
        series: [{
            name: "Brands",
            colorByPoint: true,
            data: data4
        }]
        });

// Extracting and saving data for the piechart
var category = new Array();
var ver = new Array();
var os_data = new Object();
for (i =0;i< os_temp.length;i++){

    var word = os_temp[i][0].split(" ")
    if($.inArray(word[0], category) == -1){
      var cat = word[0]
      var version = word[word.length-1]
      os_data[cat] = new Object();
      os_data[cat][version] = os_temp[i][1]
      os_data[cat]["total"] = os_temp[i][1]
      category.push(cat)

     } else{
     var cat = word[0]
     var version = word[word.length-1]
     os_data[cat][version] = os_temp[i][1]
     os_data[cat]["total"] = os_temp[i][1] + os_data[cat]["total"]


     }




}



// Pie chart

$(function () {

    var colors = Highcharts.getOptions().colors,
        categories = ['MSIE', 'Firefox', 'Chrome', 'Safari', 'Opera'],

        browserData = [],
        versionsData = [],
        i,
        j,
        brightness;


    // Build the data arrays
        for (i = 0; i < category.length; i += 1) {

        // add browser data
        browserData.push({
            name: category[i],
            y: os_data[category[i]]["total"],
            color: colors[i]

        });
        // add version data
        drillDataLen = os_data[category[i]].length;
        var version = Object.keys(os_data[category[i]])
        for (j = 0; j < version.length; j += 1) {
            brightness = 0.2 - ((j / drillDataLen) / 2);
            if (version[j] != "total"){
            versionsData.push({
                name: version[j],
                y: os_data[category[i]][version[j]],
                 color: Highcharts.Color(colors[i]).brighten(brightness).get()  });
            }
        }
    }


    // Create the chart
    $('#piechart').highcharts({
        chart: {
            type: 'pie'
        },
        title: {
            text: 'Operating System of Devices'
        },
        subtitle: {
            text: ''
        },
        yAxis: {
            title: {
                text: 'Total percent market share'
            }
        },
        plotOptions: {
            pie: {
                shadow: false,
                center: ['50%', '50%']
            }
        },
        tooltip: {
            valueSuffix: ' users '
        },
        series: [{
            name: 'Browsers',
            data: browserData,
            size: '60%',
            dataLabels: {
                formatter: function () {
                    return this.y > 5 ? this.point.name : null;
                },
                color: 'green',
                distance: -30
            }
        }, {
            name: 'Versions',
            data: versionsData,
            size: '80%',
            innerSize: '60%',
            dataLabels: {
                formatter: function () {
                    // display only if larger than 1
                    return this.y > 10 ? '<b>' + this.point.name + ':</b> ' + this.y : null;
                }
            }
        }]
    });
});
// Counts Table for Device TYpe
var vm_data = new Array();
for (i =0;i< vm_js.length;i++){

	vm_data[i] = new Array()
    vm_data[i].push(vm_js[i],vm_c_js[i])

	}
$(document).ready(function() {
    $('#devicetable').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="devicedata"></table>' );

    $('#devicedata').dataTable( {
        "data": device_js,
        "columns": [
            { "title": "Device" },
            { "title": "Count" }

        ]
    } );
    $('#vmtable').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="vmdata"></table>' );

    $('#vmdata').dataTable( {
        "data": vm_data,
        "columns": [
            { "title": "Device" },
            { "title": "Count" }

        ]
    } );
    $('#errortable').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="errordata"></table>' );

    $('#errordata').dataTable( {
        "data": error_js,
        "columns": [
            { "title": "Timestamp" },
            { "title": "Error" },
            { "title": "Link" }

        ]
    } );
} );
var sample = new Array()
for (i =0;i< country_js[0].length;i++){
        var temp = new Object()
        temp.code = String(country_js[0][i])
	    temp.z = countryc_js[0][i]
        sample.push(temp)

        }
console.log(sample)
$(function () {
        var mapData = Highcharts.geojson(Highcharts.maps['custom/world']);

        // Correct UK to GB in data

        $("#mapchart1").highcharts('Map', {
            chart : {
                borderWidth : 1
            },

            title: {
                text: 'User Distribution'
            },

            subtitle : {
                text : 'Demo of Highcharts map with bubbles'
            },

            legend: {
                enabled: false
            },

            mapNavigation: {
                enabled: true,
                buttonOptions: {
                    verticalAlign: 'bottom'
                }
            },

            series : [{
                name: 'Countries',
                mapData: mapData,
                color: '#E0E0E0',
                enableMouseTracking: false
            }, {
                type: 'mapbubble',
                mapData: mapData,
                name: 'Visits',
                joinBy: ['iso-a2', 'code'],
                data: sample,
                minSize: '4%',
                maxSize: '12%',
                tooltip: {
                    pointFormat: '{point.code}: {point.data} Visitors'
                }
            }]
        });

    });



