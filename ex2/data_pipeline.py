#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Sequence, Protocol
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

    def ingest(self, data: Union[Sequence[Union[int, float]],
                                 int,  float]) -> None:
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        csv_lst = []
        for d in data:
            index, value = d
            csv_lst.append(value)
        print(",".join(csv_lst))


class JSONPlugin():
    def process_output(self, data: list[tuple[int, str]]) -> None:
        items = [f'"item_{i}": "{v}"' for i, v in data]
        print("JSON Output:")
        print("{" + ", ".join(items) + "}")


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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            lst_output = []
            for r in range(nb):
                if not proc.file:
                    break
                lst_output.append(proc.output())
            plugin.process_output(lst_output)

    def print_processors_stats(self) -> None:
        for proc in self.processors:
            name = proc.__class__.__name__
            print(f"{name}: total {proc.total_item} items processed, "
                  f"remaining {len(proc.file)} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")

    data_csv = ['Hello world', [3.14, -1, 2.71],
                [{'log_level': 'WARNING', 'log_message':
                  'Telnet access! Use ssh instead'}, {
                      'log_level': 'INFO', 'log_message':
                      'User wil is connected'}], 42, ['Hi', 'five']]
    print("Initialize Data Stream...")
    manager = DataStream()
    print("== DataStream statistics ==")
    manager.process_stream(data_csv)
    print("Registering Processors\n")
    lst_proc = [NumericProcessor(), TextProcessor(), LogProcessor()]
    for lst in lst_proc:
        manager.register_processor(lst)
    print(f"Send first batch of data on stream: {data_csv}")
    manager.process_stream(data_csv)
    print("\n== DataStream statistics ==")
    manager.print_processors_stats()
    nb_csv = 3

    print(f"\nSend {nb_csv} processed data from each processor "
          "to a CSV plugin:")
    manager.output_pipeline(nb_csv, CSVPlugin())

    print("\n== DataStream statistics ==")
    manager.print_processors_stats()

    data_json = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                 [{'log_level': 'ERROR', 'log_message': '500 server crash'}, {
                     'log_level': 'NOTICE', 'log_message':
                     'Certificate expires in 10 days'}], [
                         32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data:{data_json}")
    nb_json = 5
    manager.process_stream(data_json)

    print("\n== DataStream statistics ==")
    manager.print_processors_stats()

    print(f"\nSend {nb_json} processed data from each processor to a "
          "JSON plugin:")
    manager.output_pipeline(nb_json, JSONPlugin())
    print("\n== DataStream statistics ==")
    manager.print_processors_stats()
