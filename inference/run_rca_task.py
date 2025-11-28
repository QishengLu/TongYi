import os
import json
import argparse
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add inference directory to sys.path so we can import modules from it
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from react_agent import MultiTurnReactAgent

def read_problem_description(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        # Naive parsing of the python string assignment
        if 'TASK_DESCRIPTION = """' in content:
            content = content.split('TASK_DESCRIPTION = """')[1]
            if '"""' in content:
                content = content.split('"""')[0]
        return content.strip()

def main():
    parser = argparse.ArgumentParser(description="Run RCA Task")
    parser.add_argument("--planning_port", type=int, default=8000, help="Port of the LLM server")
    parser.add_argument("--model", type=str, default="Qwen/Qwen2.5-72B-Instruct", help="Model name")
    args = parser.parse_args()

    # Setup paths
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # DeepResearch root
    problem_file = os.path.join(base_dir, "question_3", "problem.json")
    output_dir = os.path.join(base_dir, "output")
    working_dir = os.path.join(base_dir, "question_3")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Read problem description
    if not os.path.exists(problem_file):
        print(f"Error: Problem file not found at {problem_file}")
        return

    question = read_problem_description(problem_file)
    # Append instruction about data location in sandbox
    question += "\n\nIMPORTANT: The parquet files are located in the '/data' directory. When writing code, always use absolute paths starting with '/data/' (e.g., '/data/logs.parquet'). Do not look in the current directory."
    print(f"Loaded task description from {problem_file}")

    # Change working directory to where parquet files are
    print(f"Changing working directory to {working_dir}")
    os.chdir(working_dir)

    llm_cfg = {
        'model': args.model,
        'generate_cfg': {
            'max_input_tokens': 32000,
            'max_retries': 3,
            'temperature': 0.0,
            'top_p': 0.95,
            'presence_penalty': 1.1
        },
        'model_type': 'qwen_dashscope'
    }

    agent = MultiTurnReactAgent(
        llm=llm_cfg,
        function_list=["list_parquet_files", "get_parquet_schema", "query_parquet", "PythonInterpreter"]
    )

    data = {
        'item': {
            'question': question,
            'answer': "Unknown"
        },
        'planning_port': args.planning_port
    }

    print("Starting Agent execution...")
    try:
        result = agent._run(data, args.model)
        
        # Save result
        output_file = os.path.join(output_dir, "result.json")
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Execution completed. Result saved to {output_file}")
        
        if result.get('prediction'):
            print("\nPrediction:")
            print(result['prediction'])
            
    except Exception as e:
        print(f"Error running agent: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
