// Adding toggle by Material-UI

import React, { useState, useEffect} from 'react';
import Switch from '@material-ui/core/Switch';
import InputSlider from '../slider/slider';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { styled } from '@mui/material/styles';
import FormGroup from '@mui/material/FormGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import FormControl from '@mui/material/FormControl';

import {fetchCurrentValueFromAdafruit, addNewValueToAdafruit} from '../../utils/devices';

const AntSwitch = styled(Switch)(({ theme }) => ({
    width: 56, // Increased from 28
    height: 32, // Increased from 16
    padding: 0,
    display: 'flex',
    '&:active': {
      '& .MuiSwitch-thumb': {
        width: 30, // Increased for the active state
      },
      '& .MuiSwitch-switchBase.Mui-checked': {
        transform: 'translateX(18px)', // Adjusted for the larger size
      },
    },
    '& .MuiSwitch-switchBase': {
      padding: 4, // Increased from 2
      '&.Mui-checked': {
        transform: 'translateX(24px)', // Adjusted for the larger size
        color: '#fff',
        '& + .MuiSwitch-track': {
          opacity: 1,
          backgroundColor: theme.palette.mode === 'dark' ? '#177ddc' : '#ff6000',
        },
      },
    },
    '& .MuiSwitch-thumb': {
      boxShadow: '0 4px 8px 0 rgb(0 35 11 / 20%)', // Adjusted for the larger thumb
      width: 24, // Increased from 12
      height: 24, // Increased from 12
      borderRadius: 12, // Increased to maintain the circular shape
      transition: theme.transitions.create(['width'], {
        duration: 200,
      }),
    },
    '& .MuiSwitch-track': {
      borderRadius: 32 / 2, // Adjusted for the larger track
      opacity: 1,
      backgroundColor: theme.palette.mode === 'dark' ? 'rgba(255,255,255,.35)' : 'rgba(0,0,0,.25)',
      boxSizing: 'border-box',
    },
  }));
const Toggle = () => {
  const [powerOn, setPowerOn] = useState(false);
  const [powerOn1, setPowerOn1] = useState(false);
  const [powerOn2, setPowerOn2] = useState(false);

  const feedKeys = ['smarthome.led', 'smarthome.thieftcontrol', 'smarthome.doorcontrol']; // Example feed keys for devices

  useEffect(() => {
    // Function to fetch the current toggle states
    const fetchToggleStates = () => {
        feedKeys.forEach((key, index) => {
            fetchCurrentValueFromAdafruit(key).then(({ lastValue }) => {
                if (index === 0) setPowerOn(lastValue === 'ON');
                if (index === 1) setPowerOn1(lastValue === 'ON');
                if (index === 2) setPowerOn2(lastValue === 'ON');
            }).catch(error => console.error(`Error fetching state for ${key}:`, error));
        });
    };

    // Initial fetch
    fetchToggleStates();

    // Set interval to fetch every 3 seconds
    const intervalId = setInterval(fetchToggleStates, 3000);

    // Cleanup function to clear the interval
    return () => clearInterval(intervalId);
}, [feedKeys]);

  const updateAdafruit = async (key, newState) => {
      try {
          await addNewValueToAdafruit(key, newState ? 'ON' : 'OFF');
          console.log(`Updated ${key} to ${newState}`);
      } catch (error) {
          console.error(`Error updating ${key}:`, error);
      }
  };

  const handleToggle = () => {
      const newState = !powerOn;
      updateAdafruit(feedKeys[0], newState);
      setPowerOn(newState);
  };

  const handleToggle1 = () => {
      const newState = !powerOn1;
      updateAdafruit(feedKeys[1], newState);
      setPowerOn1(newState);
  };

  const handleToggle2 = () => {
      const newState = !powerOn2;
      updateAdafruit(feedKeys[2], newState);
      setPowerOn2(newState);
  };

    return (
        <FormControl  component="fieldset">
                <FormGroup aria-label="position" row>
                <FormControlLabel
                    value="top"
                    control={<AntSwitch
                        checked={powerOn}
                        color="primary"
                        name="powerToggle"
                        inputProps={{ 'aria-label': 'power toggle' }}
                        onChange={handleToggle}
                        ></AntSwitch>}
                        label={
                            <Typography sx={{ fontSize: '1.5rem', fontFamily: 'Nunito', padding:0, boxSizing: 'border-box' }} >
                              Light
                            </Typography>
                          }
                    
                    labelPlacement="top"
                />
                <FormControlLabel
                    value="top"
                    control={<AntSwitch
                        checked={powerOn1}
                        color="primary"
                        name="powerToggle"
                        inputProps={{ 'aria-label': 'power toggle' }}
                        onChange={handleToggle1}
                        ></AntSwitch>}
                    label={
                        <Typography sx={{ fontSize: '1.5rem', fontFamily: 'Nunito', padding:0, boxSizing: 'border-box' }} >
                          Sensor
                        </Typography>
                      }
                    labelPlacement="top"
                />
                <FormControlLabel
                    value="top"
                    control={<AntSwitch
                        checked={powerOn2}
                        color="primary"
                        name="powerToggle"
                        inputProps={{ 'aria-label': 'power toggle' }}
                        onChange={handleToggle2}
                        ></AntSwitch>}
                    label={
                        <Typography sx={{ fontSize: '1.5rem', fontFamily: 'Nunito', padding:0, boxSizing: 'border-box' }} >
                          Door
                        </Typography>
                      }
                    labelPlacement="top"
                />
                
                </FormGroup>
            </FormControl>
            
    );
};

export default Toggle;
