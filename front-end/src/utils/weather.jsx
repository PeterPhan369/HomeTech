export async function getDataHumi() {
    const response = await fetch("https://io.adafruit.com/api/v2/hungpham1406/feeds/humid");
    const data = await response.json();
    return data;
}

export async function getDataTemp() {
    const response = await fetch("https://io.adafruit.com/api/v2/hungpham1406/feeds/temp");
    const data = await response.json();
    // Assume convertUTCtoLocal is moved here too
    data.updated_at = convertUTCtoLocal(data.updated_at);
    return data;
}

export async function getDataLight() {
    const response = await fetch("https://io.adafruit.com/api/v2/hungpham1406/feeds/light");
    const data = await response.json();
    return data;
}

export function convertUTCtoLocal(utcDateString) {
    // Parse the UTC date string
    const date = new Date(utcDateString);
  
    // Add the timezone offset (+7 hours)
    // Note: getTimezoneOffset returns the difference in minutes, so you need to adjust it accordingly.
    // Since you want to specifically add 7 hours regardless of the local timezone, we directly add 7 hours.
    date.setHours(date.getHours() + 7);
  
    // Format the date to the desired format: "YYYY-MM-DD HH:MM:SS"
    const formattedDate = date.toISOString().replace('T', ' ').substring(0, 19);
  
    return formattedDate;
}