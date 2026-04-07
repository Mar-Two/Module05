#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union
from collections import deque


class DataProcessor(ABC):
    def __init__(self):
        self.file = deque()
        self.count = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        ...

    @abstractmethod
    def ingest(self, data: Any) -> None:
        ...

    def output(self) -> tuple[int, str]:
        if not self.file:
            raise ValueError("No data available")
        value = self.file.popleft()
        index = self.count
        self.count += 1
        return (index, value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if (isinstance(data, (int, float))
            or (isinstance(data, list) and
                all(isinstance(x, (int, float)) for x in data))):
            return True
        return False

    def ingest(self, data: Union[List[Union[int, float]], int, float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for x in data:
                self.file.append(str(x))
        else:
            self.file.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if (isinstance(data, str)
            or (isinstance(data, list)
                and all(isinstance(x, str) for x in data))):
            return True
        return False

    def ingest(self, data: Union[List[str], str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for x in data:
                self.file.append(x)
        else:
            self.file.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_dict(d):
            return (
                isinstance(d, dict)
                and "log_level" in d
                and "log_message" in d
                and isinstance(d["log_level"], str)
                and isinstance(d["log_message"], str)
            )
        if isinstance(data, dict):
            return is_valid_dict(data)
        if isinstance(data, list):
            return all(is_valid_dict(x) for x in data)
        return False

    def ingest(self, data:
               Union[List[Dict[str, str]], Dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for x in data:
                self.file.append(f"{x['log_level']}: {x['log_message']}")
        else:
            self.file.append(f"{data['log_level']}: {data['log_message']}")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num = NumericProcessor()
    print(f" Trying to validate input '42': {num.validate(42)}")
    print(f" Trying to validate input 'Hello': {num.validate('Hello')}")
    try:
        print(" Test invalid ingestion of string 'foo' without "
              "prior validation:")
        num.ingest("foo")
    except ValueError as e:
        print(f" Got exception: {e}")

    try:
        data_num: List[int] = [1, 2, 3, 4, 5]
        print(f" Processing data: {data_num}")
        num.ingest(data_num)
        print(" Extracting 3 values...")
        for x in range(3):
            index, value = num.output()
            print(f" Numeric value {index}: {value}")
    except ValueError as e:
        print(f" Got exception: {e}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f" Trying to validate input '42': {text.validate(42)}")

    try:
        data_txt: List[str] = ['Hello', 'Nexus', 'World']
        print(f" Processing data: {data_txt}")
        text.ingest(data_txt)
        print(" Extracting 1 value...")
        index, value = text.output()
        print(f" Text value {index}: {value}")
    except ValueError as e:
        print(f" Got exception: {e}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    try:
        data_dict: List[dict[str, str]] = [{'log_level': 'NOTICE',
                                            'log_message':
                                            'Connection to server'},
                                           {'log_level': 'ERROR',
                                            'log_message':
                                            'Unauthorized access!!'}]
        print(f" Processing data: {data_dict}")
        print(" Extracting 2 values...")
        log.ingest(data_dict)
        for x in range(2):
            index, value = log.output()
            print(f" Log entry {index}: {value}")
    except ValueError as e:
        print(f" Got exception: {e}")
