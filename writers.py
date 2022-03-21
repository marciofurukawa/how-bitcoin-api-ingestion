import os
import json
import datetime
from typing import List


class DataTypeNotSupportedForIngestionException(Exception):
    """DateType not supported for Ingestion Exception.

    Args:
        Exception (class): Exception class

    [Author] Marcio Furukawa Campos
    [date] 2022-03-20
    """

    def __init__(self, data):
        """Class initialization method.

        Args:
            data (obj): Data that can't be read

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        self.data = data
        self.message = f"Data type {type(data)} is not supported \
            for ingestion!"
        super().__init__(self.message)


class DataWriter:
    """Data Writter Class.

    [Author] Marcio Furukawa Campos
    [date] 2022-03-20
    """

    def __init__(self, coin: str, api: str) -> None:
        """Class initialization method.

        Args:
            coin (str): coin abbreviation
            api (str): api name

        [Author] Marcio Furukawa Campos
        [date] 2022-03-20
        """
        self.coin = coin
        self.api = api
        self.filename = "{api}/{coin}/{datetime}.json".format(
            api=self.api,
            coin=self.coin,
            datetime=datetime.datetime.now(),
        )

    def _write_row(self, row: str) -> None:
        """Internal method that writes a row inside the filename.
        Appends the data.

        Args:
            row (str): row to be written
        """
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "a") as f:
            f.write(row)

    def write(self, data: [List, dict]) -> None:
        """Public method to write data inside a file. It supports only
        dict and List, otherwise it throws a custom Exception.

        Args:
            data (List, dict]): data to be written

        Raises:
            DataTypeNotSupportedForIngestionException: DateType not
                                supported, it must be a dict or a List
        """
        if isinstance(data, dict):
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)
