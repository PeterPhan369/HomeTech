import React, { useEffect, useState } from 'react';
import { getDataTemp } from "../utils/weather";
import Swal from 'sweetalert2';

const Warning = () => {
  // State to store the last temperature value
  const [lastTempValue, setLastTempValue] = useState(null);

  useEffect(() => {
    const fetchTempData = async () => {
      try {
        const data = await getDataTemp(); // Assuming getDataTemp is an async function
        if (data && data.last_value > 28) {
          // Trigger an alert if last temperature value is greater than 30
          Swal.fire({
            icon: 'warning',
            title: 'Nhà của bạn đang quá nóng',
            text: 'Hãy kiểm tra nguồn điện và thiết bị điện tử quanh bạn',
          });
        }
        setLastTempValue(data.last_value); // Update state with the last temperature value
      } catch (error) {

        // Show a warning message if there is an error fetching the data
        
      }
    };
    const intervalId = setInterval(fetchTempData, 8000); // Fetch data every 3 seconds
    return () => clearInterval(intervalId); // Cleanup function to clear the interval
  }, []); // Empty dependency array means this effect runs once after the initial render

  return(
    <></>
  )
}

export default Warning;
