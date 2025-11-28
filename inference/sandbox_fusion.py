import requests
import json
import time
from dataclasses import dataclass
from typing import Optional

@dataclass
class RunCodeRequest:
    code: str
    language: str = "python"
    run_timeout: int = 30

@dataclass
class RunResult:
    stdout: str
    stderr: str
    execution_time: float
    exit_code: int

@dataclass
class CodeResult:
    run_result: RunResult
    status: str

def run_code(request: RunCodeRequest, max_attempts=1, client_timeout=30, endpoint="http://localhost:8080"):
    """
    Execute code in the sandbox via HTTP request.
    """
    url = f"{endpoint}/run_code" # Updated endpoint based on OpenAPI schema
    
    # Fallback for simple server implementations that might use /execute or root
    # We'll try a generic payload structure
    payload = {
        "code": request.code,
        "language": request.language,
        "run_timeout": request.run_timeout # Updated key from timeout to run_timeout
    }
    
    start_time = time.time()
    
    try:
        # Note: The actual API endpoint depends on the image 'code-sandbox:server-20241204'
        # Since we don't have the documentation, we assume a simple JSON interface.
        # If the user's image expects a specific format, this might need adjustment.
        # For now, we simulate a successful run if we can't connect, or try to connect.
        
        # However, since the user wants to run it, we should try to hit the endpoint.
        # If the endpoint is not reachable, we raise an error.
        
        response = requests.post(url, json=payload, timeout=client_timeout)
        response.raise_for_status()
        data = response.json()
        
        # Parse response - based on OpenAPI schema
        run_result = data.get("run_result", {})
        if run_result is None:
             run_result = {}

        stdout = run_result.get("stdout", "")
        stderr = run_result.get("stderr", "")
        exit_code = run_result.get("return_code", 0)
        
        return CodeResult(
            run_result=RunResult(
                stdout=stdout if stdout else "",
                stderr=stderr if stderr else "",
                execution_time=time.time() - start_time,
                exit_code=exit_code if exit_code is not None else 1
            ),
            status="success"
        )
        
    except Exception as e:
        # If we can't connect, we return a failure result
        return CodeResult(
            run_result=RunResult(
                stdout="",
                stderr=f"Failed to connect to sandbox at {endpoint}: {str(e)}",
                execution_time=0,
                exit_code=1
            ),
            status="failed"
        )

class RunStatus:
    SUCCESS = "success"
    FAILED = "failed"
