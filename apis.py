from abc import ABC, abstractmethod
import logging
import requests
import datetime
from backoff import on_exception, expo
import ratelimit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class MercadoBitCoinApi(ABC):
    """Abstract Class of Mercado BitCoin API.

    [Author] Marcio Furukawa Campos
    [date] 2022-03-20
    """

    def __init__(self, coin: str) -> None:
        """Class initialization method.

        Args:
            coin (str): Coin abbreviation, ie: BTC for BitCoin

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        self.coin = coin
        self.base_endpoint = "https://www.mercadobitcoin.net/api"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        """Internal abstract method to implement the endpoint.

        Returns:
            str: endpoint

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        pass

    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        """Gets the information from API.

        Returns:
            dict: json returned from API calling

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}")

        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()


class DaySummaryApi(MercadoBitCoinApi):
    """DaySummary API Class.

    Args:
        MercadoBitCoinApi (class): Abstract Class

    [Author] Marcio Furukawa Campos
    [date] 2022-03-20
    """

    type = "day-summary"

    def _get_endpoint(self, date: datetime.date) -> str:
        """Returns the Day Summary endpoint.

        Args:
            date (datetime.date): trade date

        Returns:
            str: Day Summary endpoint
        """
        endpoint = "{base}/{coin}/{type}/{year}/{month}/{day}".format(
            base=self.base_endpoint,
            coin=self.coin,
            type=self.type,
            year=date.year,
            month=date.month,
            day=date.day
        )

        return endpoint


class TradesApi(MercadoBitCoinApi):
    """Trades API Class.

    Args:
        MercadoBitCoinApi (class): Abstract Class

    [Author] Marcio Furukawa Campos
    [date] 2022-03-20
    """

    type = "trades"

    def _get_unix_epoch(self, date: datetime.datetime) -> int:
        """Returns the unix epoch of a passed date.

        Args:
            date (datetime.datetime): date to be converted

        Returns:
            int: unix epoch

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        return int(date.timestamp())

    def _get_endpoint(
        self,
        date_from: datetime.datetime = None,
        date_to: datetime.datetime = None
    ) -> str:
        """Returns the Trades endpoint.

        Args:
            date_from (datetime.datetime, optional): initial date
            date_to (datetime.datetime, optional): final date

        Returns:
            str: Trades endpoint

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        if date_from and not date_to:
            # without date_to returns 1000 trades from date_from passed
            unix_date_from = self._get_unix_epoch(date_from)
            endpoint = "{base}/{coin}/{type}/{date_from}".format(
                base=self.base_endpoint,
                coin=self.coin,
                type=self.type,
                date_from=unix_date_from,
            )
        elif date_from and date_to:
            unix_date_from = self._get_unix_epoch(date_from)
            unix_date_to = self._get_unix_epoch(date_to)
            endpoint = "{base}/{coin}/{type}/{date_from}/{date_to}".format(
                base=self.base_endpoint,
                coin=self.coin,
                type=self.type,
                date_from=unix_date_from,
                date_to=unix_date_to,
            )
        else:
            # without period returns last 1000 trades
            endpoint = f"{self.base_endpoint}/{self.coin}/{self.type}"

        return endpoint
