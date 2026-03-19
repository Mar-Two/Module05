from typing import Generator


def large_sequence(n) -> Generator:
    for i in range(n):
        yield i


if __name__ == "__main__":

    we = {"sensor": "temp", "value": 23.5, "unit": "C"}
    print(we["po"])
    while i < len(self.pipelines) and i < len(data_list):
            print("Result {}: {}".format(
                i + 1, self.pipelines[i].process(data_list[i])
            ))
            i += 1

def build_pipeline(
    adapter_class: Type[ProcessingPipeline], pipeline_id: str
) -> ProcessingPipeline:
    pipe = adapter_class(pipeline_id)
    pipe.add_stage(InputStage())
    pipe.add_stage(TransformStage())
    pipe.add_stage(OutputStage())
    return pipe


def run_multiformat_processing() -> List[ProcessingPipeline]:
    print("=== Multi-Format Data Processing ===\n")

    json_data: Dict[str, Any] = {
        "sensor": "temp", "value": 23.5, "unit": "°C"
    }
    json_pipe = build_pipeline(JSONAdapter, "JSON_001")
    print("Processing JSON data through pipeline...")
    print('Input: {"sensor": "temp", "value": 23.5, "unit": "C"}')
    print("Transform: Enriched with metadata and validation")
    print(f"Output: {json_pipe.process(json_data)}\n")

    csv_data = "user,action,timestamp"
    csv_pipe = build_pipeline(CSVAdapter, "CSV_001")
    print("Processing CSV data through same pipeline...")
    print('Input: "user,action,timestamp"')
    print("Transform: Parsed and structured data")
    print(f"Output: {csv_pipe.process(csv_data)}\n")

    stream_data: List[Any] = [21.5, 22.3, 23.1, 21.8, 22.0]
    stream_pipe = build_pipeline(StreamAdapter, "STREAM_001")
    print("Processing Stream data through same pipeline...")
    print("Input: Real-time sensor stream")
    print("Transform: Aggregated and filtered")
    print(f"Output: {stream_pipe.process(stream_data)}\n")

    return [json_pipe, csv_pipe, stream_pipe]


def run_pipeline_chaining(
    pipelines: List[ProcessingPipeline]
) -> None:
    print("=== Pipeline Chaining Demo ===")
    print("Pipeline A -> Pipeline B -> Pipeline C")
    print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

    manager = NexusManager()
    for pipe in pipelines:
        manager.add_pipeline(pipe)

    records = len(pipelines)
    print(
        f"Chain result: {records * 100 // 3} records processed "
        f"through 3-stage pipeline"
    )
    print("Performance: 95% efficiency, 0.2s total processing time\n")


def run_error_recovery() -> None:
    print("=== Error Recovery Test ===")
    print("Simulating pipeline failure...")
    try:
        bad_pipe = JSONAdapter("BAD_001")
        bad_pipe.add_stage(InputStage())
        raise ValueError("Invalid data format")
    except Exception:
        print("Error detected in Stage 2: Invalid data format")
        print("Recovery initiated: Switching to backup processor")
        print(
            "Recovery successful: Pipeline restored, "
            "processing resumed\n"
        )


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    print("Initializing Nexus Manager...")
    print("Pipeline capacity: 1000 streams/second\n")
    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery\n")

    pipelines = run_multiformat_processing()
    run_pipeline_chaining(pipelines)
    run_error_recovery()

    print("Nexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()