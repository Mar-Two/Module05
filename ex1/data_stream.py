#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union
from collections import deque


class DataProcessor(ABC):
    def __init__(self):
        self.file = deque()
        self.count = 0
        self.total_item = 0

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
                self.total_item += 1
        else:
            self.file.append(str(data))
            self.total_item += 1


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
                self.total_item += 1
                self.file.append(x)
        else:
            self.file.append(data)
            self.total_item += 1


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
                self.total_item += 1
                self.file.append(f"{x['log_level']}: {x['log_message']}")
        else:
            self.total_item += 1
            self.file.append(f"{data['log_level']}: {data['log_message']}")


class DataStream():
    def __init__(self):
        self.processors = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        if not self.processors:
            print("No processor found, no data\n")
            return
        for elem in stream:
            error = 0
            for proc in self.processors:
                if proc.validate(elem):
                    proc.ingest(elem)
                    error = 1
                    break
            if error == 0:
                print(f"DataStream error - Can't process element in "
                      f"stream: {elem}")

    def print_processors_stats(self) -> None:
        for proc in self.processors:
            name = proc.__class__.__name__
            print(f"{name}: total {proc.total_item} items processed, "
                  f"remaining {len(proc.file)} on processor")


def only_numeric_proc(data: Any) -> None:
    manager = DataStream()
    print("Registering Numeric Processor\n")
    print(f"Send first batch of data on stream: {data}")
    manager.register_processor(NumericProcessor())
    manager.process_stream(data)
    print("== DataStream statistics ==")
    manager.print_processors_stats()


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")
    manager = DataStream()
    data = ['Hello world', [3.14, -1, 2.71],
            [{'log_level': 'WARNING',
              'log_message':
              'Telnet access! Use ssh instead'}, {
                  'log_level': 'INFO', 'log_message':
                  'User wil isconnected'}], 42, ['Hi', 'five']]
    manager.process_stream(data)

    only_numeric_proc(data)
    print("\nRegistering other data processors")
    print("Send the same batch again")
    manager = DataStream()
    lst_proc = [NumericProcessor(), TextProcessor(), LogProcessor()]
    for lst in lst_proc:
        manager.register_processor(lst)
    manager.process_stream(data)
    print("== DataStream statistics ==")
    manager.print_processors_stats()
    num_remain = [1, 1, 1]
    print(f"\nConsume some elements from the data processors: Numeric "
          f"{num_remain[0]}, Text {num_remain[1]}, Log {num_remain[2]}")
    for proc, num in zip(lst_proc, num_remain):
        for r in range(num):
            proc.output()
    print("== DataStream statistics ==")
    manager.print_processors_stats()
