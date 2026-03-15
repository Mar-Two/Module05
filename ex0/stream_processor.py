from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    def format_output(self, result: str) -> str:
        pass


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        len_data: int = len(data)
        if len_data == 0:
            return False
        return isinstance(data, List) and all(isinstance(x, int) for x in data)

    def process(self, data: Any) -> str:
        len_data: int = len(data)
        sum_data: int = sum(data)
        avg_data: float = sum_data / len_data
        return (f"Processed {len_data} numeric values, sum={sum_data}, "
                f"avg={avg_data:.1f}")

    def format_output(self, result: str) -> str:
        return f"Output: {super().format_output(result)}"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        if ':' in data:
            return False
        return True

    def process(self, data: Any) -> str:
        len_char = len(data)
        len_str = len(data.split())
        return f"Processed text: {len_char} characters, {len_str} words"

    def format_output(self, result):
        return f"Output: {super().format_output(result)}"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        list_data = data.split(':')
        prefix_list = ["INFO", "ERROR", "WARNING"]
        len_data = len(list_data)
        if (len_data == 2 and any(list_data[0] == x for x in prefix_list)):
            return True
        else:
            return False

    def process(self, data: Any) -> str:
        list_data = data.split(':')
        if list_data[0] == "ERROR" or list_data[0] == "WARNING":
            return f"[ALERT] {list_data[0]} level detected:{list_data[1]}"
        else:
            return f"[INFO] {list_data[0]} level detected:{list_data[1]}"

    def format_output(self, result):
        return f"Output: {super().format_output(result)}"


if __name__ == "__main__":

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    try:
        print("Initializing Numeric Processor...")
        numeric = NumericProcessor()
        data_num = [1, 2, 3, 4, 5]
        print(f"Processing data: {data_num}")
        if numeric.validate(data_num):
            print("Validation: Numeric data verified")
        else:
            print("ERROR: Numeric data is invalidate")
        print(numeric.format_output(numeric.process(data_num)))
    except (TypeError, AttributeError, ValueError) as e:
        print(f"ERROR: {e}")
    try:
        text = TextProcessor()
        print("\nInitializing Text Processor...")
        data_text = "Hello Nexus World"
        print(f"Processing data: \"{data_text}\"")
        if text.validate(data_text):
            print("Validation: Text data verified")
        else:
            print("ERROR: Text entry is invalidate")
        print(text.format_output(text.process(data_text)))
    except (TypeError, AttributeError, ValueError) as e:
        print(f"ERROR: {e}")
    try:
        print("\nInitializing Log Processor...")
        log = LogProcessor()
        log_data = "ERROR: Connection timeout"
        print(f"Processing data: \"{log_data}\"")
        if log.validate(log_data):
            print("Validation: Log entry verified")
        else:
            print("ERROR: Log entry is invalidate")
        print(log.format_output(log.process(log_data)))
    except (TypeError, AttributeError, ValueError) as e:
        print(f"ERROR: {e}")

    print("\n=== Polymorphic Processing Demo ===")
    list_processor = [NumericProcessor(), TextProcessor(), LogProcessor()]
    list_data = [[1, 2, 3, 4], "Hello World", "INFO: system ready", 123,
                 "WARNING: Time freeze", ["L", "L", "h"], 1111111]
    i = 1
    for data in list_data:
        for processor in list_processor:
            try:
                if processor.validate(data):
                    print(f"Result {i}: {processor.process(data)}")
                    i += 1
                    break
            except (TypeError, AttributeError) as e:
                print(f"EXCEPTION: {e}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")
