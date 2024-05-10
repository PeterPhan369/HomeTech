// Adding toggle by Material-UI

import React, { useState } from 'react';
import Switch from '@material-ui/core/Switch';
import InputSlider from '../slider/slider';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import { styled } from '@mui/material/styles';
import Toggle from '../switch/switch';
import FormGroup from '@mui/material/FormGroup';
import FormControlLabel from '@mui/material/FormControlLabel';
import FormControl from '@mui/material/FormControl';

const Control = () => {
    const [powerOn, setPowerOn] = useState(false);
    function handleToggle(){
        setPowerOn(!powerOn);
        console.log("3434")
    };


    return (
        <Box sx={{ display: 'flex', justifyContent: 'center' , marginBottom: '100px'}}>
            <Box sx={{alignContent:'center'}}>
                <Toggle/>
                <InputSlider/>
            </Box>
        </Box>
        
        
    );
};

export default Control;
