import React, { useEffect } from 'react';
import addNotification from "react-push-notification";
import { getDataTemp } from "../../utils/weather"; // Adjust the import path as necessary

// Notification function as defined before
export function notify() {
    addNotification({
        title: 'Temperature Alert',
        subtitle: 'High Temperature Detected',
        message: 'The temperature has exceeded 50°C. Please take necessary precautions.',
        theme: 'darkblue',
        native: true // When using native, your OS will handle theming.
    });
}

// Component to check temperature and potentially show a notification
export default function TemperatureChecker() {
    useEffect(() => {
        // Function to fetch temperature and show notification if condition is met
        const checkTemperature = async () => {
            try {
                const tempData = await getDataTemp(); // Fetch temperature data
                if (tempData.last_value > 20) { // Check if temperature exceeds 50
                    console.log("dfsdfsd")
                    notify(); // Show notification if condition is met
                }
            } catch (error) {
                console.error("Failed to fetch temperature data", error);
            }
        };

        checkTemperature();

        // Optional: Set an interval to periodically check the temperature
        // const interval = setInterval(checkTemperature, 60000); // Check every minute
        // return () => clearInterval(interval);
    }, []);

    return (
        <div className="page">
          <button onClick={notify} className="button">
           Hello world.
          </button>
      </div>
    );
}
