import pandas as pd
from flask import request, jsonify
from app import webserver



# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")
        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}
        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    if request.method == 'GET':
    # Check if job_id is valid   
        return jsonify(webserver.tasks_runner.get_task_status(job_id))

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs_response():
    if request.method == 'GET':
    # Check if job_id is valid
        return jsonify(webserver.tasks_runner.get_tasks_statuses())

@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs_response():
    if request.method == 'GET':
    # Check if job_id is valid
        statuses = webserver.tasks_runner.get_tasks_statuses()
        return jsonify({"running_jobs":sum(1 for j in statuses.values() if j['status'] == "running")})


@webserver.route('/api/graceful_shutdown', methods=['GET'])
def get_graceful_shutdown_response():
    if request.method == 'GET':
    # Check if job_id is valid
        statuses = webserver.tasks_runner.get_tasks_statuses()
        running_statuses = sum(1 for j in statuses.values() if j['status'] == "running")
        try:
            if running_statuses == 0:
                webserver.tasks_runner.stop()
                return jsonify({ "job_id": -1, "reason":"shutting down" })
        except Exception as e:
            return jsonify({{ "job_id": -1, "error":"Error while shutting down :{e}" }})                  

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    if request.method == 'POST':
        # print(webserver.job_counter,'MBA')
        # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
      
        def calc_states_mean():
            filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
            average_values = filtered_by_question.groupby('LocationDesc')['Data_Value'].mean()
            return average_values

        webserver.tasks_runner.submit_task(calc_states_mean,webserver.job_counter)
        webserver.job_counter += 1
        # Increment job_id counter
        # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
    # Register job. Don't wait for task to finish
    # Increment job_id counter
        def calc_state_mean():
            filtered_by_question_and_state = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])& 
            (webserver.data_ingestor.df['LocationDesc'] == data['state'])]
            average_values = filtered_by_question_and_state.groupby('LocationDesc')['Data_Value'].mean() 
            return average_values
           
        webserver.tasks_runner.submit_task(calc_state_mean,webserver.job_counter)
        webserver.job_counter +=1 
       # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_best_mean():
            if data['question'] in webserver.data_ingestor.questions_best_is_max:
                filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
                average_values_max = filtered_by_question.groupby('LocationDesc')['Data_Value'].mean() 
                average_values_max = average_values_max.nlargest(5)
                return average_values_max

            elif data['question'] in webserver.data_ingestor.questions_best_is_min:
                filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
                average_values_min = filtered_by_question.groupby('LocationDesc')['Data_Value'].mean()
                average_values_min = average_values_min.nsmallest(5)
                return average_values_min

        webserver.tasks_runner.submit_task(calc_best_mean,webserver.job_counter)
        webserver.job_counter +=1 
       # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
     if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_worst_mean():
            if data['question'] in webserver.data_ingestor.questions_best_is_max:
                filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
                average_values_max = filtered_by_question.groupby('LocationDesc')['Data_Value'].mean() 
                average_values_max = average_values_max.nsmallest(5)
                return average_values_max
                
            elif data['question'] in webserver.data_ingestor.questions_best_is_min:
                filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
                average_values_min = filtered_by_question.groupby('LocationDesc')['Data_Value'].mean()
                average_values_min = average_values_min.nlargest(5)
                return average_values_min

        webserver.tasks_runner.submit_task(calc_worst_mean,webserver.job_counter)
        webserver.job_counter +=1 
       # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
     if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_global_mean():
            filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
            average_values = filtered_by_question['Data_Value'].mean() 
            result = pd.Series({"global_mean":average_values})
            return result
           
        webserver.tasks_runner.submit_task(calc_global_mean,webserver.job_counter)
        webserver.job_counter +=1 
        # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})
       
@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
     if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_diff_from_global_mean():
            filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])]
            global_average_values = filtered_by_question['Data_Value'].mean()
            state_average_values = filtered_by_question.groupby('LocationDesc')["Data_Value"].mean() 
            average = global_average_values - state_average_values
            return average

        webserver.tasks_runner.submit_task(calc_diff_from_global_mean,webserver.job_counter)
        webserver.job_counter +=1 
       # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
     if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_state_diff_from_global_mean():
            filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'])== data['question']]
            filtered_by_question_and_state = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'] == data['question'])
            & (webserver.data_ingestor.df['LocationDesc'] == data['state'])]
            global_average_values = filtered_by_question['Data_Value'].mean()
            state_average_values = filtered_by_question_and_state.groupby('LocationDesc')["Data_Value"].mean() 
            average = global_average_values-state_average_values
            return average
           
        webserver.tasks_runner.submit_task(calc_state_diff_from_global_mean,webserver.job_counter)
        webserver.job_counter += 1
        # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_mean_by_category():
            filtered_by_question = webserver.data_ingestor.df[(webserver.data_ingestor.df['Question'])== data['question']]
            average_values = filtered_by_question.groupby(['LocationDesc','StratificationCategory1','Stratification1'])['Data_Value'].mean()
            average_values_df = average_values.reset_index()
            result_dict = dict(zip(zip(average_values_df['LocationDesc'], average_values_df['StratificationCategory1'], average_values_df['Stratification1']), average_values_df['Data_Value']))
            result_dict_str = {str(key): value for key, value in result_dict.items()}
            return result_dict_str
           
        webserver.tasks_runner.submit_task(calc_mean_by_category,webserver.job_counter)
        webserver.job_counter +=1 
        # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
     if request.method == 'POST':
    # Get request data
        data = request.json
        print(f"Got request {data}")
        # Register job. Don't wait for task to finish
        # Increment job_id counter
        def calc_mean_by_category():
            filtered_by_question_and_state = webserver.data_ingestor.df[((webserver.data_ingestor.df['Question'])== data['question'])
            & ((webserver.data_ingestor.df['LocationDesc'])== data['state'])]
            average_values = filtered_by_question_and_state.groupby(['StratificationCategory1','Stratification1'])['Data_Value'].mean()
            result_dict = {data['state']: {}}
            for (category, stratification), value in average_values.items():
                result_dict[data['state']][f"('{category}', '{stratification}')"] = value
            return result_dict
           
        webserver.tasks_runner.submit_task(calc_mean_by_category,webserver.job_counter)
        webserver.job_counter +=1 
       # Return associated job_id.Preferred to use webserver.job_counter variable to avoid using another initialization 
        return jsonify({"job_id":webserver.job_counter-1})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
