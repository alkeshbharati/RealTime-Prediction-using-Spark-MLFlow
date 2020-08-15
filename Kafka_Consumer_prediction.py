import faust
from typing import List
import requests
import json

app = faust.App(
    'flight_prediction_app',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)

kafka_topic = app.topic('flight')

@app.agent(kafka_topic)
async def process(flights):
    async for value in flights:
        result = requests.post('http://127.0.0.1:5000/invocations', json=json.loads(value))
        print('Input data: ' + str(value))
        print('Flight arrival delay result: ' + str(result.json()))

if __name__ == '__main__':    
    # run the consumer
    app.main()
