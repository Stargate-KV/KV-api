import React, { useState } from 'react';
import Operations from './Components/Operations';
import Evaluation from './Components/Evaluation';
import Auth from './Components/Authentication';
import {Typography, FormControl, InputLabel, Select, MenuItem, FormControlLabel, Switch, Grid} from '@material-ui/core';

function App() {
    const [mode, setMode] = useState('Production');
    const [cache, setCache] = useState(false);
    const [authMessage, setAuthMessage] = useState(''); // Define the authMessage state here

    return (
        <div style={{ padding: '50px' }}>
            <Grid container justify="center" style={{ marginBottom: '30px' }}>
                <Typography variant="h4">
                    Stargate Key-Value API
                </Typography>
            </Grid>
            <Auth authMessage={authMessage} setAuthMessage={setAuthMessage}/>
            <h2> Step2: Select the Mode of Usage</h2>
            <FormControl variant="outlined" style={{ marginBottom: '20px' }}>
                <InputLabel id="mode-select-label">Mode</InputLabel>
                <Select labelId="mode-select-label" value={mode} onChange={e => setMode(e.target.value)} label="Mode">
                    <MenuItem value="Evaluation">Evaluation</MenuItem>
                    <MenuItem value="Production">Production</MenuItem>
                </Select>
            </FormControl>
            <h2> Step3: Choose the Usage of Cache</h2>
            <div>
                <FormControlLabel
                    control={<Switch checked={cache} onChange={e => setCache(e.target.checked)} />}
                    label="Use Cache"
                />
            </div>
            <h2> Step4: Select the Operation Type</h2>
            <div>
                {mode === 'Production' ? <Operations authToken={authMessage} /> : <Evaluation authToken={authMessage} />}
            </div>
        </div>
    );
}

export default App;
