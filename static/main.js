var data1 = new Array();
var data2 = new Array();
var data3 = new Array();
var data4 = new Array();
var data5 = new Array();
var data6 = new Array();
var data7 = new Array();
var data8 = new Array();
var data9 = new Array();
var data10 = new Array();
for (i = 0; i < parseInt(0.70*time_js.length); i++){
    data1[i] = new Array();
    data1[i].push((time_js[i]*1000)+19800000,val1_js[i])
    }
var j = parseInt(0.70*time_js.length)+1;
for (i = 0; i < val2_js.length; i++){
    data2[i] = new Array();
    data2[i].push((time_js[j]*1000)+19800000,val2_js[i])
    j++;
    }

for (i = 0; i < parseInt(0.70*time_js.length); i++){
    data3[i] = new Array();
    data3[i].push((time_js[i]*1000)+19800000,val3_js[i])
    }
var j = parseInt(0.70*time_js.length)+1;
for (i = 0; i < val4_js.length; i++){
    data4[i] = new Array();
    data4[i].push((time_js[j]*1000)+19800000,val4_js[i])
    j++;
    }

for (i = 0; i < parseInt(0.70*time_js.length); i++){
    data5[i] = new Array();
    data5[i].push((time_js[i]*1000)+19800000,val5_js[i])
    }
var j = parseInt(0.70*time_js.length)+1;
for (i = 0; i < val2_js.length; i++){
    data6[i] = new Array();
    data6[i].push((time_js[j]*1000)+19800000,val6_js[i])
    j++;
    }

for (i = 0; i < parseInt(0.70*time_js.length); i++){
    data7[i] = new Array();
    data7[i].push((time_js[i]*1000)+19800000,val7_js[i])
    }
var j = parseInt(0.70*time_js.length)+1;
for (i = 0; i < val4_js.length; i++){
    data8[i] = new Array();
    data8[i].push((time_js[j]*1000)+19800000,val8_js[i])
    j++;
    }

for (i = 0; i < parseInt(0.70*time_js.length); i++){
    data9[i] = new Array();
    data9[i].push((time_js[i]*1000)+19800000,val9_js[i])
    }
var j = parseInt(0.70*time_js.length)+1;
for (i = 0; i < val2_js.length; i++){
    data10[i] = new Array();
    data10[i].push((time_js[j]*1000)+19800000,val10_js[i])
    j++;
    }

$(function () {         
var chart = new Highcharts.Chart({
                        chart: {
                            renderTo: 'container2',
                            type: 'spline'
                        },
                
                        title: {
                            text: 'Predicted Bytes',
                            x: -20 //center
                        },
                        rangeSelector: {
                            selected: 1
                        },
                        subtitle: {
                            text: 'Epoch time data'
                        },
                        xAxis: {
                            title: {
                            text: 'TIME'
                            }
                            
                        },
                        yAxis: {
                            title: {
                            text: 'TOTAL BYTES TRANSFERED'
                            },
                            min: 0
                        },
                        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +' Bytes transferred<br/>' ;
        }
        },
                         plotOptions: {
                            spline: {
                                lineWidth: 0.7,
                                states: {
                                    hover: {
                                        lineWidth: 1
                                    }
                                },
                                marker: {
                                    enabled: false
                                }
                            }
                        },
                        series: [{
                            name: 'Bytes Original',
                            data: data1
                        }, {
                            name: 'Bytes Predicted',
                            data: data2
                        }]
                    });
                });

$(function () {         
var chart = new Highcharts.Chart({
                        chart: {
                            renderTo: 'container3',
                            type: 'spline'
                        },
                
                        title: {
                            text: 'Predicted GETS',
                            x: -20 //center
                        },
                        rangeSelector: {
                            selected: 1
                        },
                        subtitle: {
                            text: 'Epoch time data'
                        },
                        xAxis: {
                            title: {
                            text: 'TIME'
                            }
                        },
                        yAxis: {
                            title: {
                            text: 'TOTAL GET REQUESTS'
                            },
                            min: 0
                        },
                        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +' request<br/>' ;
        }
        },
                        plotOptions: {
                            spline: {
                                lineWidth: 0.7,
                                states: {
                                    hover: {
                                        lineWidth: 1
                                    }
                                },
                                marker: {
                                    enabled: false
                                }
                            }
                        },                       
                        series: [{
                            name: 'GETS Original',
                            data: data3
                        }, {
                            name: 'GETS Predicted',
                            data: data4
                        }]
                    });
                });

$(function () {         
var chart = new Highcharts.Chart({
                        chart: {
                            renderTo: 'container4',
                            type: 'spline'
                        },
                
                        title: {
                            text: 'Predicted POSTS',
                            x: -20 //center
                        },
                        rangeSelector: {
                            selected: 1
                        },
                        subtitle: {
                            text: 'Epoch time data'
                        },
                        xAxis: {
                            title: {
                            text: 'TIME'
                            }
                        },
                        yAxis: {
                            title: {
                            text: 'TOTAL POST REQUESTS'
                            },
                            min: 0
                        },
                        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +' request<br/>' ;
        }
        },
                        plotOptions: {
                            spline: {
                                lineWidth: 0.7,
                                states: {
                                    hover: {
                                        lineWidth: 1
                                    }
                                },
                                marker: {
                                    enabled: false
                                }
                            }
                        },                       
                        series: [{
                            name: 'POSTS Original',
                            data: data5
                        }, {
                            name: 'POSTS Predicted',
                            data: data6
                        }]
                    });
                });

$(function () {         
var chart = new Highcharts.Chart({
                        chart: {
                            renderTo: 'container5',
                            type: 'spline'
                        },
                
                        title: {
                            text: 'Predicted REQUESTS',
                            x: -20 //center
                        },
                        rangeSelector: {
                            selected: 1
                        },
                        subtitle: {
                            text: 'Epoch time data'
                        },
                        xAxis: {
                            title: {
                            text: 'TIME'
                            }
                        },
                        yAxis: {
                            title: {
                            text: 'TOTAL REQUESTS'
                            },
                            min: 0
                        },
                        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +' request<br/>' ;
        }
        },
                        plotOptions: {
                            spline: {
                                lineWidth: 0.7,
                                states: {
                                    hover: {
                                        lineWidth: 1
                                    }
                                },
                                marker: {
                                    enabled: false
                                }
                            }
                        },                       
                        series: [{
                            name: 'Requests Original',
                            data: data7
                        }, {
                            name: 'Requests Predicted',
                            data: data8
                        }]
                    });
                });

$(function () {         
var chart = new Highcharts.Chart({
                        chart: {
                            renderTo: 'container1',
                            type: 'spline'
                        },
                
                        title: {
                            text: 'Predicted TRAFFIC',
                            x: -20 //center
                        },
                        rangeSelector: {
                            selected: 1
                        },
                        subtitle: {
                            text: 'Epoch time data'
                        },
                        xAxis: {
                            title: {
                            text: 'TIME'
                            }
                        },
                        yAxis: {
                            title: {
                            text: 'TOTAL TRAFFIC (VISITS)'
                            },
                            min: 0
                        },
                        tooltip: {
            headerFormat: '<b>{series.name}</b><br>',
            pointFormat: '{point.x:%e-%b %H:%M}:  {point.y:.2f} hits',
            formatter: function()
        {
            return Highcharts.dateFormat('%d.%m.%Y %H.%M', this.x) +': '+ this.y +' visits<br/>' ;
        }
        },
                        plotOptions: {
                            spline: {
                                lineWidth: 0.7,
                                states: {
                                    hover: {
                                        lineWidth: 1
                                    }
                                },
                                marker: {
                                    enabled: false
                                }
                            }
                        },                       
                        series: [{
                            name: 'VISITS Original',
                            data: data9
                        }, {
                            name: 'VISITS Predicted',
                            data: data10
                        }]
                    });
                });



 var value = 1; // Here you have to set value that you get from database
        var $select = $('.js-select'); // Specify your select here

        var updateBoxes = function( value ) {
            $('.js-box').hide();
            $('#container' + value).show();
        }

        var initSelects = function( value ) {
            if (value !== 0) {
                $select.val( value );
            }
            $select.on('change', function() {
                updateBoxes( $select.val() );
            });
        }

        initSelects( value );
        updateBoxes( value );