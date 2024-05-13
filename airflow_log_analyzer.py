from pathlib import Path
import re

#Step 1: Iterate through the root directory and  get all the log files
def get_log_files(log_dir):
    # Create a Path object for the log directory
    log_path = Path(log_dir)

    # Use rglob to recursively list all files ending with ".log"
    log_files = log_path.rglob('*.log')

    return log_files

# raw_logs = [
#     "[2024-05-13T11:13:48.271-0700] {taskinstance.py:2890} ERROR - Task failed with exception",
#     "[2024-05-13T11:13:48.291-0700] {standard_task_runner.py:110} ERROR - Failed to execute job 19 for task t0 (Bash command failed. The command returned a non-zero exit code 1.; 52158)"
# ]

#Step 2: creating a python method to parse each log file found in Step 1
def analyze_file(log_files):
    all_error_messages = []
    # Define regex pattern to match log entries
    log_pattern = r'\[(.*?)\] {(.*?):(\d+)} (ERROR) - (.*)'
   
    for log_file in log_files:
        with open(log_file, 'r') as file:
            for line in file:
                #print("Current Line:", line)  # Debugging print
                match = re.match(log_pattern, line)
                if match:
                    #print("Match Found:", match.group())  # Debugging print
                    timestamp = match.group(1)
                    file_name = match.group(2)
                    line_number = match.group(3)
                    error_type = match.group(4)
                    error_message = match.group(5)
                    all_error_messages.append({'timestamp': timestamp, 'file_name': file_name, 'line_number': line_number, 'error_type': error_type, 'error_message': error_message})
    return all_error_messages

#Step 3: Prints the cumulative info collected from all files
def print_error_messages(error_messages):
    print("Total count of error entries:", len(error_messages))
    print("Error messages:")
    for error_log in error_messages:
        print("Error Message:", error_log['error_message'])


if __name__ == "__main__":
    all_error_messages = []
    log_dir = "/Users/shilpa/install/airflow-tutorial/airflow/logs/dag_id=market_data"  # Update with the path to your log directory
    log_files = get_log_files(log_dir)
    
    print("Log files found:")
    for log_file in log_files:
        print(log_file)
        # Analyze each log file for error messages and collect them
        error_messages = analyze_file([log_file])
        all_error_messages.extend(error_messages)

    # Print out error count and the error messages
    print_error_messages(all_error_messages)


 
        