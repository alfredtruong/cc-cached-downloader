"""Multithreading Helpers.

This module contains utility functions
to manage multi-threading.

"""

from typing import Callable
from concurrent import futures
import time
import threading

def make_multithreaded(func: Callable, threads: int) -> Callable:
    """Creates a multithreaded version of a function.

    Args:
        func: Function that is meant to be executed on
            a list of input objects.
        threads: The number of threads the multithreaded
            version of the function should use.

    Returns:
        A multithreaded version of the `func` input function.

    """

    def multithreaded_function(input_list: list, *args) -> list:
        """Executes function on input list using multiple threads.

        Args:
            input_list: The list of objects a function should be
                executed on.
            *args: Variable length argument list of additional
                parameters needed for the function to be executed.

        Returns:
            List of results after all input list elements were
            processed. Input order might not be preserved in
            output list.

        """
        results = []
        input_queue = input_list[:]  # Copy of the input list

        def worker(input_item):
            """Wrap the function call in a try-except to handle exceptions."""
            try:
                return func(input_item, *args)  # Pass *args to the function
            except Exception as e:
                print(f"Error processing {input_item}: {e}")  # Print the exception
                return None  # Return None or handle as needed

        with futures.ThreadPoolExecutor(max_workers=threads) as executor:
            while input_queue:
                # Submit tasks to the executor
                future_to_input_item = {
                    executor.submit(worker, input_item): input_item for input_item in input_queue[:threads]
                }
                input_queue = input_queue[threads:]  # Remove submitted items from the queue

                # Print the number of currently running threads
                print(f"Number of running threads: {threading.active_count()}")

                for future in futures.as_completed(future_to_input_item):
                    result = future.result()
                    if result is not None:
                        if isinstance(result, list):
                            results.extend(result)
                        else:
                            results.append(result)

                # Sleep briefly to allow any new threads to be created
                time.sleep(0.1)

        return results

    return multithreaded_function
