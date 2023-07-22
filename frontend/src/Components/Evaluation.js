import React, { useState } from 'react';
import { MenuItem, Button, FormControl, InputLabel, Select, TextField } from '@material-ui/core';
import axios from 'axios';

const Evaluation = ({ authToken }) => {
    const [operation, setOperation] = useState(null);
    const [evaluationValue, setEvaluationValue] = useState(0);

    const performEvaluation = async () => {
        await axios.post('/api/evaluation', { operation, evaluationValue })
            .then(response => {
                console.log(response.data);
            });
    };

    return (
        <>
            <TextField
                label="Number of Operations"
                type="number"
                value={evaluationValue}
                onChange={e => setEvaluationValue(Number(e.target.value))}
                variant="outlined"
                style={{ marginBottom: '20px' }}
            />
            <div>
                <FormControl variant="outlined" style={{ marginBottom: '40px', minWidth: '200px'  }}>
                    <InputLabel id="operation-select-label">Operation</InputLabel>
                    <Select labelId="operation-select-label" value={operation} onChange={e => setOperation(e.target.value)} label="Operation">
                        {['put', 'get'].map(op => <MenuItem key={op} value={op}>{op}</MenuItem>)}
                    </Select>
                </FormControl>
                <Button variant="contained" color="primary" onClick={performEvaluation}>Evaluation</Button>
            </div>
        </>
    );
}

export default Evaluation;
