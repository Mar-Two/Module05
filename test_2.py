#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from typing import Protocol


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Any:
        if data:
            print(f"Input: {data}")
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        if isinstance(data, List):
            return data
        if isinstance(data, Dict):
            return data
        return "Error detected in Stage 2: Invalid data format"


class OutputStage:
    def process(self, data: Any) -> str:
        return data


class ProcessingPipeline(ABC):
    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id = pipeline_id
        self.stages: List[Any] = []

    def add_stage(self, stage: Any) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


class JSONAdapter(ProcessingPipeline):
    def process(self, data: Any) -> int:
        json_list = [x for x in data if isinstance(x, dict)]
        if not json_list:
            return 0
        error = "Error detected in Stage 2: Invalid data format"
        i = 0
        for json in json_list:
            stage_data = json
            for stage in self.stages:
                stage_data = stage.process(stage_data)
                if stage_data == error:
                    return error
            try:
                value = ""
                if "sensor" in stage_data and "value" in stage_data and "unit" in stage_data:
                    if stage_data["value"] > 30:
                        value = "High range"
                    elif stage_data["value"] < 10:
                        value = "Low range"
                    else:
                        value = "Normal range"
                    print(f"Processed temperature reading: "
                          f"{stage_data["value"]}°{stage_data["unit"]} ({value})")
                else:
                    raise KeyError()
            except KeyError:
                continue
            i += 1
        return i


class CSVAdapter(ProcessingPipeline):
    def process(self, data: Any) -> int:
        csv_list = [x for x in data if isinstance(x, str)]
        if not csv_list:
            return 0
        len_csv = len(csv_list)
        new_csv_list = [mot for x in csv_list for mot in x.split(',')]
        error = "Error detected in Stage 2: Invalid data format"
        stage_data = new_csv_list
        for stage in self.stages:
            stage_data = stage.process(stage_data)
            if stage_data == error:
                return error
        try:
            action_csv = [x for x in stage_data if x == "action"]
            len_action_csv = len(action_csv)
            print(f"User activity logged: {len_action_csv} actions processed")
        except AttributeError:
            pass
        return len_csv


class StreamAdapter(ProcessingPipeline):
    def process(self, data: Any) -> int:
        stream_list = [x for x in data if isinstance(x, (int, float))]
        if not stream_list:
            return 0
        len_stream = len(stream_list)
        error = "Error detected in Stage 2: Invalid data format"
        stage_data = stream_list
        for stage in self.stages:
            stage_data = stage.process(stage_data)
            if stage_data == error:
                return "Error detected in Stage 2: Invalid data format"
        try:
            sum_stream = sum(stage_data)
            avg_stream = sum_stream / len(stage_data)
            print(f"Stream summary: {len(stage_data)} readings, avg: "
                  f"{avg_stream:.1f}°C")
        except ZeroDivisionError:
            pass
        return len_stream


class NexusManager:
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def process_data(self, data_list: List[Any]) -> int:
        sum_all_data = 0
        for pipe in self.pipelines:
            sum_all_data += pipe.process(data_list)
        return sum_all_data


def add_stage_and_pipeline() -> Any:
    json_pipeline = JSONAdapter("json")
    csv_pipeline = CSVAdapter("csv")
    stream_pipeline = StreamAdapter("stream")
    nexusmanager = NexusManager()
    for p in [json_pipeline, csv_pipeline, stream_pipeline]:
        p.add_stage(InputStage())
        p.add_stage(TransformStage())
        p.add_stage(OutputStage())
        nexusmanager.add_pipeline(p)
    return nexusmanager


if __name__ == "__main__":
    manager = add_stage_and_pipeline()
    data = [{"sensor": "temp", "value": 33.5, "unit": "C"}, {"sensor": "temp", "value": 26.5, "unit": "F"}, 21.5, 22.3, 23.1, 21.8, 22.0,{"senso": "temp", "value": 26.5, "unit": "C"}]
    sum_data_valid = manager.process_data(data)
    res = (sum_data_valid / len(data)) * 100
    print(res)