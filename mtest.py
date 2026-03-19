from abc import ABC, abstractmethod
from typing import Any, List, Protocol, Union
import json
import csv
from collections import deque
import time


# ==============================
# Protocol for pipeline stages
# ==============================
class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


# ==============================
# Abstract Pipeline Base Class
# ==============================
class ProcessingPipeline(ABC):

    def __init__(self):
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)

    def run(self, data: Any) -> Any:
        start = time.time()

        try:
            for stage in self.stages:
                data = stage.process(data)

        except Exception as e:
            print(f"[ERROR] Stage failed: {e}")
            raise

        end = time.time()
        print(f"Pipeline executed in {end-start:.4f}s")
        return data

    @abstractmethod
    def process(self, data: Any) -> Any:
        pass


# ==============================
# Pipeline Stages
# ==============================
class InputStage:
    def process(self, data: Any) -> Any:
        print("InputStage: validating input")
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        print("TransformStage: transforming data")

        if isinstance(data, dict):
            return {k: str(v).upper() for k, v in data.items()}

        if isinstance(data, list):
            return [str(x).upper() for x in data]

        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        print("OutputStage: formatting output")
        return data


# ==============================
# Data Adapters
# ==============================
class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        if isinstance(data, str):
            data = json.loads(data)

        result = self.run(data)

        return json.dumps(result)


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:

        rows = []

        if isinstance(data, str):
            reader = csv.DictReader(data.splitlines())
            rows = list(reader)

        result = self.run(rows)

        return result


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id
        self.buffer = deque()

    def process(self, data: Any) -> Union[str, Any]:

        self.buffer.append(data)

        results = []

        while self.buffer:
            item = self.buffer.popleft()
            results.append(self.run(item))

        return results


# ==============================
# Pipeline Manager
# ==============================
class NexusManager:

    def __init__(self):
        self.pipelines = {}

    def register(self, name: str, pipeline: ProcessingPipeline):
        self.pipelines[name] = pipeline

    def process(self, name: str, data: Any):

        if name not in self.pipelines:
            raise ValueError("Pipeline not found")

        print(f"Running pipeline: {name}")

        return self.pipelines[name].process(data)


# ==============================
# Example Usage
# ==============================
if __name__ == "__main__":

    manager = NexusManager()

    # JSON pipeline
    json_pipeline = JSONAdapter("json_pipeline")
    json_pipeline.add_stage(InputStage())
    json_pipeline.add_stage(TransformStage())
    json_pipeline.add_stage(OutputStage())

    manager.register("json", json_pipeline)

    # CSV pipeline
    csv_pipeline = CSVAdapter("csv_pipeline")
    csv_pipeline.add_stage(InputStage())
    csv_pipeline.add_stage(TransformStage())
    csv_pipeline.add_stage(OutputStage())

    manager.register("csv", csv_pipeline)

    # Stream pipeline
    stream_pipeline = StreamAdapter("stream_pipeline")
    stream_pipeline.add_stage(InputStage())
    stream_pipeline.add_stage(TransformStage())
    stream_pipeline.add_stage(OutputStage())

    manager.register("stream", stream_pipeline)

    # ==================
    # Test JSON
    # ==================
    json_data = '{"sensor":"temp","value":23.5,"unit":"c"}'

    print("\nProcessing JSON:")
    result = manager.process("json", json_data)
    print(result)

    # ==================
    # Test CSV
    # ==================
    csv_data = """name,score
alice,10
bob,20"""

    print("\nProcessing CSV:")
    result = manager.process("csv", csv_data)
    print(result)

    # ==================
    # Test Stream
    # ==================
    print("\nProcessing Stream:")
    result = manager.process("stream", {"event": "login", "user": "alice"})
    print(result)