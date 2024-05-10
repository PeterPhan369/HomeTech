import React, { createContext, useContext, useState } from 'react';

// Create context
const SliderContext = createContext();

// Create and export the provider component
export const SliderProvider = ({ children }) => {
    const [lastValue, setLastValue] = useState(null);

    // The context value that will be supplied to any descendants of this provider.
    const value = {
        lastValue,
        setLastValue,
    };

    return (
        <SliderContext.Provider value={value}>
            {children}
        </SliderContext.Provider>
    );
};

// Custom hook to use the slider context
export const useSlider = () => useContext(SliderContext);
