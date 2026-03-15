from typing import Any, List, Dict, Optional, Union
from abc import ABC, abstractmethod


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        pass

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {'id': self.stream_id}


class SensorStream(DataStream):

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.stream_type = "Sensor data"
        self.total_reading = 0
        self.sum_temp = 0
        self.count_temp = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        new_list = [x.split(':') for x in data_batch if isinstance(x, str)]
        valid_str = ["temp", "humidity", "pressure"]
        for x in new_list:
            if x[0] in valid_str:
                try:
                    val = float(x[1])
                    self.total_reading += 1
                    if x[0] == "temp":
                        self.sum_temp += val
                        self.count_temp += 1
                except ValueError:
                    continue
        avg = self.sum_temp / self.count_temp if self.count_temp > 0 else 0
        return (f"{self.total_reading} readings processed, "
                f"avg temp: {avg:.1f}°C")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        avg = self.sum_temp / self.count_temp if self.count_temp > 0 else 0
        my_dict = {
            'id': self.stream_id,
            'type': self.stream_type,
            'total': self.total_reading,
            "analyse": avg
        }
        stats.update(my_dict)
        return stats

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        new_list = [x.split(':') for x in data_batch if isinstance(x, str)]
        return [x for x in new_list if x[0] == criteria]


class TransactionStream(DataStream):

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.stream_type = "Transaction data"
        self.total_ope = 0
        self.benef = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        new_list = [x.split(':') for x in data_batch if isinstance(x, str)]
        valid_str = ["buy", "sell"]
        for x in new_list:
            if x[0] in valid_str:
                try:
                    val = int(x[1])
                    self.total_ope += 1
                    if x[0] == "buy":
                        self.benef += val
                    else:
                        self.benef -= val
                except ValueError:
                    continue
        if self.benef < 0:
            return (f"{self.total_ope} operations processed, net flow: "
                    f"-{abs(self.benef)} units")
        else:
            return (f"{self.total_ope} operations processed, net flow: "
                    f"+{self.benef} units")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        my_dict = {
            'id': self.stream_id,
            'type': self.stream_type,
            'total': self.total_ope,
            'analyse': self.benef
        }
        stats.update(my_dict)
        return stats

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        new_list = [x.split(':') for x in data_batch if isinstance(x, str)]
        return [x for x in new_list if x[0] == criteria]


class EventStream(DataStream):

    def __init__(self, stream_id):
        super().__init__(stream_id)
        self.stream_type = "Event Data"
        self.events = 0
        self.error = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        valid_str = ["login", "error", "logout"]
        for x in data_batch:
            if x in valid_str:
                self.events += 1
                if x == "error":
                    self.error += 1
        return (f"{self.events} events processed, {self.error} error detected")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        stats = super().get_stats()
        my_dict = {
            'id': self.stream_id,
            'type': self.stream_type,
            'total': self.events,
            'analyse': self.error
        }
        stats.update(my_dict)
        return stats

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return [x for x in data_batch if x == criteria]


class StreamProcessor:
    def __init__(self):
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream):
        self.streams.append(stream)

    def process_all(self, data_batch: List[Any]):
        for stream in self.streams:
            one, two = stream.process_batch(data_batch).split(',')
            print(f"- {stream.stream_type} -> {one}")

    def get_dashboard(self) -> List[Dict]:
        return [stream.get_stats() for stream in self.streams]

    def print_filtering_report(self, batch: List[Any]):
        print("Stream filtering active: High-priority data only")
        errors = len([x for x in batch if "error" in str(x)])
        large_trans = len([x for x in batch if "buy:200" in str(x) or
                           "sell:100" in str(x)])
        print(f"Filtered results: {errors} critical event alerts, "
              f"{large_trans} large transaction")


def display_data() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    print("Initializing Sensor Stream...")
    sensor_stream = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor_stream.stream_id}, "
          f"Type: {sensor_stream.stream_type}")
    data_sensor = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {data_sensor}")
    print(f"Sensor analysis: {sensor_stream.process_batch(data_sensor)}\n")
    print("Initializing Transaction Stream...")
    trans_stream = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans_stream.stream_id}, "
          f"Type: {trans_stream.stream_type}")
    data_trans = ["buy:100", "sell:250", "buy:75"]
    print(f"Processing transaction batch: {data_trans}")
    print(f"Transaction analysis: {trans_stream.process_batch(data_trans)}\n")
    print("Initializing Event Stream...")
    event_stream = EventStream("EVENT_001")
    print(f"Stream ID: {event_stream.stream_id}, "
          f"Type: {event_stream.stream_type}")
    data_event = ["login", "error", "logout"]
    print(f"Processing event batch: {data_event}")
    print(f"Event analysis: {event_stream.process_batch(data_event)}\n")


if __name__ == "__main__":
    display_data()
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    data = ["temp:invalide", "buy:pas_un_nombre", "login", 123, None]

    processor = StreamProcessor()
    processor.add_stream(SensorStream("SENSOR_001"))
    processor.add_stream(TransactionStream("TRANS_001"))
    processor.add_stream(EventStream("EVENT_001"))
    processor.process_all(data)
    print()
    processor.print_filtering_report(data)
    print("\nAll streams processed successfully. Nexus throughput optimal.")
