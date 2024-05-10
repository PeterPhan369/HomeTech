import axios from 'axios';

export async function fetchCurrentValueFromAdafruit(feedKey) {
    const ADAFRUIT_IO_USERNAME = 'PhanDao';
    const ADAFRUIT_IO_KEY = 'aio_rbXg65mhjzWKcbzIrjt2JOavlKkH';
    const url = `https://io.adafruit.com/api/v2/${ADAFRUIT_IO_USERNAME}/feeds/${feedKey}/data/last`;

    try {
        const response = await axios.get(url, {
            headers: {
                'X-AIO-Key': ADAFRUIT_IO_KEY,
            },
        });
        // Uncomment below to log the raw response data
        // console.log("Adafruit Response Data:", response.data);

        const lastValue = response.data.value;
        const updatedAt = response.data.updated_at;

        return {
            lastValue,
            updatedAt: new Date(updatedAt)
        };
    } catch (error) {
        console.error('Error fetching current value from Adafruit:', error);
        throw new Error('Unable to fetch current value from Adafruit');
    }
}

export async function addNewValueToAdafruit(feedKey, newValue) {
    const ADAFRUIT_IO_USERNAME = 'PhanDao';
    const ADAFRUIT_IO_KEY = 'aio_rbXg65mhjzWKcbzIrjt2JOavlKkH';
    const url = `https://io.adafruit.com/api/v2/${ADAFRUIT_IO_USERNAME}/feeds/${feedKey}/data/last`;

    try {
        let response = await axios.get(url, {
            headers: {
                'X-AIO-Key': ADAFRUIT_IO_KEY,
            },
        });
        // Uncomment below to log the raw response data
        // console.log("Adafruit Response Data:", response.data);

        const toggleValue = {
            value: newValue,
        };

        response = await axios.post(url.replace('/last', ''), toggleValue, {
            headers: {
                'Content-Type': 'application/json',
                'X-AIO-Key': ADAFRUIT_IO_KEY,
            }
        });

        // Uncomment below to log the response of POST request
        // console.log("POST Response Data:", response.data);

        return toggleValue;
    } catch (error) {
        console.error('Error adding new value to Adafruit:', error);
        throw new Error('Unable to add new value to Adafruit');
    }
}
