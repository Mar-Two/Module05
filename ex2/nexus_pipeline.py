from typing import Protocol
from typing import Any, Dict
from abc import ABC, abstractmethod


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class InputStage:
    def process(self, data: Any) -> Dict:
        pass 


class TransformStage:
    def process(self, data: Any) -> Dict:
        pass


class OutputStage:
    def process(self, data: Any) -> str:
        pass


class ProcessingPipeline(ABC):
    def __init__(self):
        self.stages = []

    def add_stage(self, stage: Any) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        current_data = data
        for stage in self.stages:
            current_data = stage.process(current_data)
        return current_data




if __name__ == "__main__":
    stage = ProcessingPipeline()
    stage.display()
    stage.add_stage(InputStage())
    stage.display()