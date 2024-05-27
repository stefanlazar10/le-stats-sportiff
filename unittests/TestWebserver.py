import unittest
from app import webserver
from flask import Flask
from routes import states_mean_request
from app import 

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app = webserver.test_client()
 
    def test_states_mean_request(self):
        # Data used for the request
        data = {
            "question":"Percent of adults aged 18 years and older who have an overweight classification"
        }

        # POST request to the route
        with self.app.test_request_context('/api/states_mean'):
            response = states_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)

    def test_state_mean_request(self):
        # Data used for the request
        data = {
            "question":"Percent of adults aged 18 years and older who have an overweight classification","state":"Guam"
        }

        # POST request to the route
        with self.app.test_request_context('/api/state_mean'):
            response = state_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)
    
    def test_best5_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)"
        }

        # POST request to the route
        with self.app.test_request_context('/api/best5'):
            response = best_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)        

    def test_worst5_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults aged 18 years and older who have obesity"
        }

        # POST request to the route
        with self.app.test_request_context('/api/worst5'):
            response = worst_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)                
    
    def test_global_mean_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        }

        # POST request to the route
        with self.app.test_request_context('/api/global_mean'):
            response = global_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)
                
    def test_diff_from_mean_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults aged 18 years and older who have an overweight classification"
        }

        # POST request to the route
        with self.app.test_request_context('/api/diff_from_mean'):
            response = diff_from_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)

    def test_state_diff_from_mean_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults who report consuming vegetables less than one time daily", "state": "Virgin Islands"
        }

        # POST request to the route
        with self.app.test_request_context('/api/state_diff_from_mean'):
            response = state_diff_from_mean_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)
    
    def test_mean_by_category_request(self):
        # Data used for the request
        data = {
           "question": "Percent of adults aged 18 years and older who have an overweight classification"
        }

        # POST request to the route
        with self.app.test_request_context('/api/mean_by_category'):
            response = mean_by_category_request()

        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)

    def test_state_mean_by_category_request(self):
        # Data used for the request
        data = {
            "question": "Percent of adults aged 18 years and older who have an overweight classification", "state": "Oklahoma"
        }
        # POST request to the route
        with self.app.test_request_context('/api/state_mean_by_category'):
            response = state_mean_by_category_request()
        # Verify that the response contains a valid job_id
        self.assertIn("job_id", response.json)            


if __name__ == '__main__':
    unittest.main()       