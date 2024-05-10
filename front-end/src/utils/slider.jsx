// utils/storage.js
export function getSliderValue() {
    const value = localStorage.getItem('sliderValue');
    console.log(value)
    return value ? Number(value) : 0; // Convert to number and provide a default if null
  }
  