from abc import ABC
from typing import Protocol, Any, List, Dict, Union, Generator
from collections import defaultdict


# -----------------------------
# ProcessingStage Protocol
# -----------------------------

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


# -----------------------------
# Pipeline Stages
# -----------------------------

class InputStage:
    def process(self, data: Any) -> Any:
        print("InputStage -> validation")
        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        print("TransformStage -> transformation")

        if isinstance(data, dict):
            return {k: v for k, v in data.items()}

        if isinstance(data, list):
            return [x for x in data]

        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        print("OutputStage -> output formatting")
        return data


# -----------------------------
# Abstract Processing Pipeline
# -----------------------------

class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: List[ProcessingStage] = []
        self.stats = defaultdict(int)

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def process(self, data: Any) -> Any:

        try:
            for stage in self.stages:
                data = stage.process(data)
                self.stats["stages"] += 1

        except Exception as e:
            print(f"[{self.pipeline_id}] error:", e)
            raise

        self.stats["records"] += 1
        return data

    def get_stats(self) -> Dict[str, int]:
        return dict(self.stats)


# -----------------------------
# JSON Adapter (simple dict)
# -----------------------------

class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)

    def process(self, data: Any) -> Dict[str, Any]:
        if isinstance(data, Dict):
            print(f"\nJSON pipeline -> {self.pipeline_id}")
            return super().process(data)


# -----------------------------
# CSV Adapter
# -----------------------------

class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)

    def process(self, data: str) -> List[List[str]]:

        print(f"\nCSV pipeline -> {self.pipeline_id}")

        rows = data.split("\n")
        parsed = [row.split(",") for row in rows]

        return super().process(parsed)


# -----------------------------
# Stream Adapter (generator)
# -----------------------------

class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str):
        super().__init__(pipeline_id)

    def process(self, stream: Generator[Any, None, None]) -> List[Any]:

        print(f"\nStream pipeline -> {self.pipeline_id}")

        results = []

        for data in stream:
            processed = super().process(data)
            results.append(processed)

        return results


# -----------------------------
# Nexus Manager
# -----------------------------

class NexusManager:

    def __init__(self):
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline):
        self.pipelines.append(pipeline)

    def run(self, data: Any):

        results = []

        for pipeline in self.pipelines:
            try:
                result = pipeline.process(data)
                results.append(result)

            except Exception as e:
                print("Pipeline failed:", e)

        return results

    def stats(self):

        print("\n--- Statistics ---")

        for p in self.pipelines:
            print(p.pipeline_id, p.get_stats())


# -----------------------------
# Stream generator
# -----------------------------

def live_stream() -> Generator[Dict[str, int], None, None]:

    events = [
        {"temp": 21},
        {"temp": 22},
        {"temp": 23},
        {"temp": 24},
    ]

    for e in events:
        print("Streaming data:", e)
        yield e


# -----------------------------
# Demo
# -----------------------------

def main():

    print("\n=== NEXUS PIPELINE SYSTEM ===")

    manager = NexusManager()

    json_pipeline = JSONAdapter("JSON_PIPELINE")
    csv_pipeline = CSVAdapter("CSV_PIPELINE")
    stream_pipeline = StreamAdapter("STREAM_PIPELINE")

    for p in [json_pipeline, csv_pipeline, stream_pipeline]:

        p.add_stage(InputStage())
        p.add_stage(TransformStage())
        p.add_stage(OutputStage())

    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    json_data = {"sensor": "temp", "value": 23}
    csv_data = "user,action\nalice,login\nbob,logout"
    stream_data = live_stream()
    data = [json_data, csv_data, stream_data]
    manager.run(data)

    manager.stats()

    print("\nSystem complete")


if __name__ == "__main__":
    main()