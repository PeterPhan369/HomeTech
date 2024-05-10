// useDataFetch.js
import { useState, useEffect } from 'react';

export function convertGMT(dateString) {
  const date = new Date(dateString + 'Z'); // Ensure the input is treated as UTC
  date.setHours(date.getHours() + 7); // Add 7 hours
  // Format the date to a more readable form, local time assumed
  const updatedDateString = date.toISOString().replace('T', ' ').substring(0, 19);
  return updatedDateString;
}

export default function useDataFetch(url, intervalMs = 100000) {
  const [data, setData] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(url);
        const jsonData = await response.json();
        const updatedData = jsonData.data.map((item, index) => ({
          ...item,
          stt: index + 1,
          created_at: convertGMT(item.created_at)
        }));
        setData(updatedData);
      } catch (error) {
        console.error("Failed to fetch data", error);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, intervalMs);

    return () => clearInterval(interval);
  }, [url, intervalMs]);

  return data;
}
