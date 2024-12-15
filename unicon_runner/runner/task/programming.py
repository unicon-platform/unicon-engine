import asyncio
import uuid
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from unicon_runner.executor.variants.base import Executor, ExecutorResult, ExecutorResult
from unicon_runner.schemas import (
    File,
    ProgrammingEnvironment,
    Request,
    TaskEvalResult,
    TaskEvalStatus,
)


class Program(BaseModel):
    """Equivalent to RunnerPackage in unicon_backend"""

    model_config = ConfigDict(extra="allow")

    """used for testcase_id in unicon_backend, but the runner doesnt need to know that, just needs to parrot it back"""
    entrypoint: str
    files: list[File]

    @model_validator(mode="after")
    def check_entrypoint_exists_in_files(self) -> Self:
        if not any(file.file_name == self.entrypoint for file in self.files):
            raise ValueError(f"Entrypoint {self.entrypoint} not found in RunnerPackage files")
        return self


class Programs(BaseModel):
    submission_id: str
    environment: ProgrammingEnvironment
    programs: list[Program]

    async def run(self, executor: Executor) -> TaskEvalResult[list[ExecutorResult]]:
        results_with_index: dict[int, ExecutorResult] = {}
        async with asyncio.TaskGroup() as tg:
            for index, request in enumerate(self.programs):
                tg.create_task(self.run_program(executor, request, index, results_with_index))

        results = [results_with_index[i] for i in range(len(results_with_index))]

        return TaskEvalResult(
            submission_id=self.submission_id,
            status=TaskEvalStatus.SUCCESS,
            result=results,
        )

    async def run_program(
        self,
        executor: Executor,
        program: Program,
        index: int,
        results: dict[int, ExecutorResult],
    ):
        request = Request(**program.model_dump(), environment=self.environment)
        result = await executor.run_request(request, str(uuid.uuid4()))
        results[index] = ExecutorResult.model_validate(
            {**(program.model_extra or {}), **result.model_dump()}
        )
