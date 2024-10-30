import os
from langchain_core.language_models.llms import LLM
from typing import Any, Dict, List, Optional
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from snowflake.snowpark.session import Session
from langchain_core.callbacks import BaseCallbackHandler
from langchain.schema.output import LLMResult, Generation

import json
import time


from queue import Queue

from concurrent.futures import ThreadPoolExecutor, as_completed

DEBUG = os.getenv("DEBUG", False)


class ProgressCallback(BaseCallbackHandler):
    def __init__(self, total: int, progress_queue: Queue, **kwargs):
        super().__init__(**kwargs)
        self.started = 0
        self.finished = 0
        self.total = total + 2
        self.progress_queue = progress_queue

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> Any:
        self.started += 1
        self.progress_queue.put(("update", self.started, self.finished, self.total))

    # Override on_llm_end method. This is called after every response from LLM
    def on_llm_end(self, response: LLMResult, **kwargs) -> Any:
        self.finished += 1
        self.progress_queue.put(("update", self.started, self.finished, self.total))


class CortexLLM(LLM):
    session: Session
    max_retries = 3
    retry_delay = 10
    model = "reka-core"
    total: int = 0
    concurrency: int = 2

    def _generate(
        self,
        prompts: List[str],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> LLMResult:
        generations = []

        with ThreadPoolExecutor(
            max_workers=self.concurrency
        ) as executor:  # Limiting ThreadPoolExecutor based on concurrency
            futures = []
            for prompt in prompts:
                future = executor.submit(self._call, prompt, run_manager)
                futures.append(future)

            for future in as_completed(futures):
                response = future.result()
                generations.append([Generation(text=response)])

        response = LLMResult(generations=generations)
        return response

    def _call(
        self,
        prompt: str,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
    ) -> str:
        retries = 0
        model = self.model

        while retries < self.max_retries:
            json_response = None  # Initialize json_response variable
            try:
                if DEBUG:
                    time.sleep(2)
                    return "test"
                job = self.session.sql(
                    """
                    SELECT SNOWFLAKE.CORTEX.COMPLETE(
                        :1,
                        [
                            {'role': 'system', 'content': 'You are a helpful AI assistant. 
                                       You are tasked to help summarize and detect trends based 
                                       on the support cases provided. Only generate insights based on the content provided by the user.' },
                            {'role': 'user', 'content': :2}
                        ],
                        {
                            'max_tokens': 8000,
                            'temperature': 0.7
                        }
                    )
                    """,
                    (model, prompt),
                ).collect_nowait()
                query_id = job.query_id
                response = job.result()
                if len(response) > 0:
                    json_response = json.loads(response[0][0])
                    message = json_response["choices"][0].get("messages", "")
                    self.total += json_response["usage"]["total_tokens"]
                else:
                    message = ""
                    print("No response received from LLM: ", response)
                    print("Query ID: ", query_id)
                if len(message.strip()) > 0:
                    if run_manager:
                        run_manager.on_llm_end(message)
                    return message
                print(
                    f"Got an empty response on attempt {retries + 1} of {self.max_retries}. Model: {model}. Query ID: {query_id}"
                )
                model = "mixtral-8x7b"
                print(f"If retries left, will try with model {model}.")

            except Exception as e:
                print(
                    f"Exception occurred on attempt {retries + 1} of {self.max_retries}: {str(e)}"
                )
                if retries == self.max_retries - 1:
                    raise e
            finally:
                time.sleep(self.retry_delay)
                # slightly modify prompt to get around cache
                retries += 1
        return ""

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        """Return a dictionary of identifying parameters."""
        return {
            "model_name": "CustomCortexModel",
        }

    @property
    def _llm_type(self) -> str:
        return "cortex"

    @property
    def total_tokens(self) -> int:
        return self.total
