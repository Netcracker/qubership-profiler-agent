import ReactEcharts from "echarts-for-react";
import useCallsFetchArg from '@app/features/cdt/calls/use-calls-fetch-arg';
import { useCallsStore, useCallsStoreSelector } from '@app/features/cdt/calls/calls-store';
import { type CallInfo, useGetCallsStatisticsByConditionQuery } from '@app/store/cdt-openapi';
import classNames from "@app/features/cdt/calls/calls-chart.module.scss";
import { unix } from 'moment';
import {userLocale} from "@app/common/user-locale";

const CallsChart = () => {
    const [callRequest, {shouldSkip, notReady}] = useCallsFetchArg();
    const {isFetching, data, isError, error, refetch} = useGetCallsStatisticsByConditionQuery(callRequest, {
        skip: shouldSkip,
    });

    if (data) {
        console.log("data")
        console.log(data)
    }

    const graphCollapsed = useCallsStoreSelector(s => s.graphCollapsed);

    const dataset: (any)[][] = [];

    let min_calls = Number.MAX_SAFE_INTEGER;
    let max_calls = 0

    if (data?.calls) {
        data?.calls.forEach(elem => {
            dataset.push([elem.ts, elem.duration, elem.calls])
            if (elem.calls && elem.calls > max_calls) {
                max_calls = elem.calls
            }
            if (elem.calls && elem.calls < min_calls) {
              min_calls = elem.calls
          }
        });
    }

    const options = {
        grid: {
          left: '8%',
          top: '10%'
        },
        xAxis: {
          splitLine: {
            lineStyle: {
              type: 'dashed'
            }
          },
          name: 'Timestamp',
          type: 'time'
        },
        yAxis: {
          splitLine: {
            lineStyle: {
              type: 'dashed'
            }
          },
          scale: true,
          name: 'Duration'
        },
        dataZoom: {
            show: true,
            top: 'top'
        },
        tooltip: {
            trigger: "item"
        },
        series: [
          {
            data: dataset,
            type: 'scatter',
            symbolSize: function (data: any) {
              return 4*Math.log10((data[2] - min_calls) / (max_calls - min_calls) * 30 + 10);
            },
            emphasis: {
              focus: 'series',
              label: {
                show: true,
                formatter: function (param: any) {
                  return param.data[2];
                },
                position: 'top'
              }
            },
            itemStyle: {
              shadowBlur: 5,
              shadowColor: 'rgba(10,20,50,0.3)',
              shadowOffsetY: 2,
              color: 'rgba(145,198,245,0.5)'
            },
            tooltip: {
              formatter: (param: any) => {
                  // const ts = unix(param.data[0]);
                  const ts = unix(param.data[0]/1000).toDate().toLocaleString(userLocale, {hour12: false });
                  return [
                      '<b>' + ts + '</b> <br/>',
                      'Duration: ' + param.data[1] + ' ms<br/>',
                      'Calls: ' + param.data[2] + '<br/>'
                  ].join('');
              }
            }
          }
        ]
      };

    return (
        <>
            {!notReady && !graphCollapsed && <ReactEcharts
                option={options}
                className={classNames.graphContainer}
            ></ReactEcharts>}
        </>
    );
};

export default CallsChart;
