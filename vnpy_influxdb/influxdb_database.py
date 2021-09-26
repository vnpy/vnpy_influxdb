from datetime import datetime
from typing import List
import shelve

from influxdb_client import (
    InfluxDBClient,
    WriteApi,
    QueryApi,
    DeleteApi
)
from influxdb_client.client.write_api import SYNCHRONOUS
from pandas import DataFrame

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import (
    generate_vt_symbol,
    extract_vt_symbol,
    get_file_path
)


class InfluxdbDatabase(BaseDatabase):
    """InfluxDB数据库接口"""

    overview_filename = "influxdb_overview"
    overview_filepath = str(get_file_path(overview_filename))

    def __init__(self) -> None:
        """"""
        self.database: str = SETTINGS["database.database"]
        self.user: str = SETTINGS["database.user"]
        self.password: str = SETTINGS["database.password"]
        self.host: str = SETTINGS["database.host"]
        self.port: int = SETTINGS["database.port"]

        self.client: InfluxDBClient = InfluxDBClient(
            url=f"http://{self.host}:{self.port}",
            token=self.password,
            timeout=36000000,
            org=self.user
        )

        self.write_api: WriteApi = self.client.write_api(
            write_options=SYNCHRONOUS
        )
        self.query_api: QueryApi = self.client.query_api()
        self.delete_api: DeleteApi = self.client.delete_api()

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""
        data: List[dict] = []

        # 读取主键参数
        bar: BarData = bars[0]
        vt_symbol: str = bar.vt_symbol
        interval: Interval = bar.interval

        # 将BarData数据转换为字典，并调整时区
        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d = {
                "measurement": "bar_data",
                "tags": {
                    "vt_symbol": vt_symbol,
                    "interval": interval.value
                },
                "time": bar.datetime.isoformat(),
                "fields": {
                    "open_price": bar.open_price,
                    "high_price": bar.high_price,
                    "low_price": bar.low_price,
                    "close_price": bar.close_price,
                    "volume": bar.volume,
                    "turnover": bar.turnover,
                    "open_interest": bar.open_interest,
                }
            }
            data.append(d)

        self.write_api.write(
            bucket=self.database,
            org=self.user,
            record=data
        )

        # 更新K线汇总数据
        symbol, exchange = extract_vt_symbol(vt_symbol)
        key = f"{vt_symbol}_{interval.value}"

        f = shelve.open(self.overview_filepath)
        overview = f.get(key, None)

        if not overview:
            overview = BarOverview(
                symbol=symbol,
                exchange=exchange,
                interval=interval
            )
            overview.count = len(bars)
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
        else:
            overview.start = min(overview.start, bars[0].datetime)
            overview.end = max(overview.end, bars[-1].datetime)

            query: str = f'''
                from(bucket: "{self.database}")
                    |> range(start: 2000-01-01T00:00:00Z, stop: {datetime.now().isoformat()}Z)
                    |> filter(fn: (r) =>
                        r._measurement == "bar_data" and
                        r.interval == "{interval.value}" and
                        r.vt_symbol == "{vt_symbol}" and
                        r._field == "close_price"
                    )
                    |> count()
                    |> yield(name: "count")
            '''
            df: DataFrame = self.query_api.query_data_frame(query)

            for tp in df.itertuples():
                overview.count = tp._5

        f[key] = overview
        f.close()

        return True

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        data: List[dict] = []

        # 读取主键参数
        tick = ticks[0]
        vt_symbol: str = tick.vt_symbol

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            if not tick.localtime:
                tick.localtime = tick.datetime

            d = {
                "measurement": "tick_data",
                "tags": {
                    "vt_symbol": vt_symbol
                },
                "time": tick.datetime.isoformat(),
                "fields": {
                    "name": tick.name,
                    "volume": tick.volume,
                    "turnover": tick.turnover,
                    "open_interest": tick.open_interest,
                    "last_price": tick.last_price,
                    "last_volume": tick.last_volume,
                    "limit_up": tick.limit_up,
                    "limit_down": tick.limit_down,

                    "open_price": tick.open_price,
                    "high_price": tick.high_price,
                    "low_price": tick.low_price,
                    "pre_close": tick.pre_close,

                    "bid_price_1": tick.bid_price_1,
                    "bid_price_2": tick.bid_price_2,
                    "bid_price_3": tick.bid_price_3,
                    "bid_price_4": tick.bid_price_4,
                    "bid_price_5": tick.bid_price_5,

                    "ask_price_1": tick.ask_price_1,
                    "ask_price_2": tick.ask_price_2,
                    "ask_price_3": tick.ask_price_3,
                    "ask_price_4": tick.ask_price_4,
                    "ask_price_5": tick.ask_price_5,

                    "bid_volume_1": tick.bid_volume_1,
                    "bid_volume_2": tick.bid_volume_2,
                    "bid_volume_3": tick.bid_volume_3,
                    "bid_volume_4": tick.bid_volume_4,
                    "bid_volume_5": tick.bid_volume_5,

                    "ask_volume_1": tick.ask_volume_1,
                    "ask_volume_2": tick.ask_volume_2,
                    "ask_volume_3": tick.ask_volume_3,
                    "ask_volume_4": tick.ask_volume_4,
                    "ask_volume_5": tick.ask_volume_5,

                    "localtime": tick.localtime.timestamp()
                }
            }
            data.append(d)

        self.write_api.write(
            bucket=self.database,
            org=self.user,
            record=data
        )

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """读取K线数据"""
        vt_symbol: str = generate_vt_symbol(symbol, exchange)

        query: str = f'''
            from(bucket: "{self.database}")
                |> range(start: {start.isoformat()}Z, stop: {end.isoformat()}Z)
                |> filter(fn: (r) =>
                   r._measurement == "bar_data" and
                   r.interval == "{interval.value}" and
                   r.vt_symbol == "{vt_symbol}"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        df: DataFrame = self.query_api.query_data_frame(query)

        bars: List[BarData] = []
        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp._5.timestamp(), tz=DB_TZ)

            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                datetime=dt,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                close_price=tp.close_price,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """读取TICK数据"""
        vt_symbol: str = generate_vt_symbol(symbol, exchange)

        query: str = f'''
            from(bucket: "{self.database}")
                |> range(start: {start.isoformat()}Z, stop: {end.isoformat()}Z)
                |> filter(fn: (r) =>
                   r._measurement == "tick_data" and
                   r.vt_symbol == "{vt_symbol}"
                )
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

        df: DataFrame = self.query_api.query_data_frame(query)

        ticks: List[TickData] = []
        for tp in df.itertuples():
            dt = datetime.fromtimestamp(tp._5.timestamp(), tz=DB_TZ)

            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                name=tp.name,
                volume=tp.volume,
                turnover=tp.turnover,
                open_interest=tp.open_interest,
                last_price=tp.last_price,
                last_volume=tp.last_volume,
                limit_up=tp.limit_up,
                limit_down=tp.limit_down,
                open_price=tp.open_price,
                high_price=tp.high_price,
                low_price=tp.low_price,
                pre_close=tp.pre_close,
                bid_price_1=tp.bid_price_1,
                bid_price_2=tp.bid_price_2,
                bid_price_3=tp.bid_price_3,
                bid_price_4=tp.bid_price_4,
                bid_price_5=tp.bid_price_5,
                ask_price_1=tp.ask_price_1,
                ask_price_2=tp.ask_price_2,
                ask_price_3=tp.ask_price_3,
                ask_price_4=tp.ask_price_4,
                ask_price_5=tp.ask_price_5,
                bid_volume_1=tp.bid_volume_1,
                bid_volume_2=tp.bid_volume_2,
                bid_volume_3=tp.bid_volume_3,
                bid_volume_4=tp.bid_volume_4,
                bid_volume_5=tp.bid_volume_5,
                ask_volume_1=tp.ask_volume_1,
                ask_volume_2=tp.ask_volume_2,
                ask_volume_3=tp.ask_volume_3,
                ask_volume_4=tp.ask_volume_4,
                ask_volume_5=tp.ask_volume_5,
                localtime=datetime.fromtimestamp(tp.localtime),
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        vt_symbol: str = generate_vt_symbol(symbol, exchange)

        # 查询数量
        query1: str = f'''
            from(bucket: "{self.database}")
                |> range(start: 2000-01-01T00:00:00Z, stop: {datetime.now().isoformat()}Z)
                |> filter(fn: (r) =>
                    r._measurement == "bar_data" and
                    r.interval == "{interval.value}" and
                    r.vt_symbol == "{vt_symbol}" and
                    r._field == "close_price"
                )
                |> count()
                |> yield(name: "count")
        '''
        df: DataFrame = self.query_api.query_data_frame(query1)

        for tp in df.itertuples():
            count = tp._5

        # 删除K线数据
        self.delete_api.delete(
            "2000-01-01T00:00:00Z",
            datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            f'interval="{interval.value}" and vt_symbol="{vt_symbol}"',
            bucket=self.database,
            org=self.user
        )

        # 删除K线汇总数据
        f = shelve.open(self.overview_filepath)
        vt_symbol = generate_vt_symbol(symbol, exchange)
        key = f"{vt_symbol}_{interval.value}"
        if key in f:
            f.pop(key)
        f.close()

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        vt_symbol: str = generate_vt_symbol(symbol, exchange)

        # 查询数量
        query1: str = f'''
            from(bucket: "{self.database}")
                |> range(start: 2000-01-01T00:00:00Z, stop: {datetime.now().isoformat()}Z)
                |> filter(fn: (r) =>
                    r._measurement == "tick_data" and
                    r.vt_symbol == "{vt_symbol}" and
                    r._field == "last_price"
                )
                |> count()
                |> yield(name: "count")
        '''
        df: DataFrame = self.query_api.query_data_frame(query1)

        count = 0
        for tp in df.itertuples():
            count = tp._5

        # 删除K线数据
        self.delete_api.delete(
            "2000-01-01T00:00:00Z",
            datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            f'vt_symbol="{vt_symbol}"',
            bucket=self.database,
            org=self.user
        )

        return count

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线汇总信息"""
        f = shelve.open(self.overview_filepath)
        overviews = list(f.values())
        f.close()
        return overviews
