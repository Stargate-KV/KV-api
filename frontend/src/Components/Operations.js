import React, {useState} from 'react';
import {MenuItem, Button, FormControl, InputLabel, Select, TextField} from '@material-ui/core';
import axios from 'axios';

const Operations = ({authToken}) => {
    const [msg, setMsg] = useState('');
    const [operation, setOperation] = useState('');
    const [key, setKey] = useState('');
    const [value, setValue] = useState('');
    const [db, setDb] = useState('');
    const operations = ['put', 'get', 'deleteKey', 'deleteDB', 'createDB'];
    if (authToken) {
        authToken = JSON.parse(authToken);
        authToken = authToken.authToken;
    }

    const performOperation = async (operation, key, value, db, authToken) => {
        // The endpoint is based on the operation.
        let endpoint = `http://35.221.21.180:8080/kvstore/v1/`;

        // The request data and method depends on the operation.
        let method = 'post';
        let requestData = {};
        if (operation === 'put') {
            requestData = {key, value};
        } else if (operation === 'get' || operation === 'deleteKey') {
            requestData = {key};
        } else if (operation === 'deleteDB') {
            requestData = {db};
        }
        if (operation === 'createDB') {
            method = 'POST';
            endpoint = endpoint + `databases`;
        }
        console.log(endpoint);

        // If the authToken is used as a X-Cassandra-Token.
        const config = {
            headers: {
                'accept': 'application/json',
                'content-Type': 'application/json',
                'X-Cassandra-Token': authToken
            },
        };

        try {
            const response = await axios.post(
                'http://35.221.21.180:8081/v1/auth/token/generate',
                {},
                {
                    headers: {
                        'accept': 'application/json',
                        'content-Type': 'application/json',
                        'X-Cassandra-Token': authToken,
                    }
                }

            );
            if(typeof response.data === 'object') {
                setMsg(JSON.stringify(response.data, null, 2));
            } else {
                setMsg(response.data);
            }
            console.log(response.data);
        } catch (error) {
            console.error(error);
        }
    };

    return (
        <>
            <FormControl variant="outlined" style={{marginBottom: '20px', minWidth: '200px'}}>
                <InputLabel id="operation-select-label">Operation</InputLabel>
                <Select labelId="operation-select-label"
                        value={operation}
                        onChange={e => setOperation(e.target.value)}
                        label="Operation">
                    {operations.map(op => <MenuItem key={op} value={op}>{op}</MenuItem>)}
                </Select>
            </FormControl>
            <h3> Step4.1: Input Value and Confirm Operation</h3>
            <div>
                {['put', 'get', 'deleteKey'].includes(operation) && (
                    <TextField
                        label="Key"
                        value={key}
                        onChange={e => setKey(e.target.value)}
                        variant="outlined"
                        style={{marginBottom: '20px'}}
                    />
                )}
                {operation === 'put' && (
                    <TextField
                        label="Value"
                        value={value}
                        onChange={e => setValue(e.target.value)}
                        variant="outlined"
                        style={{marginBottom: '20px'}}
                    />
                )}
                {operation === 'createDB' && (
                    <TextField
                        label="DB Name create"
                        value={db}
                        onChange={e => setDb(e.target.value)}
                        variant="outlined"
                        style={{marginBottom: '20px'}}
                    />
                )}
                <Button variant="contained" color="primary" onClick={() => performOperation(operation, key, value, db, authToken)}>Perform Operation</Button>
            </div>
            <div>
                <h3> The Received response is:</h3>
            </div>
            <div>
                <textarea readOnly value={msg} style={{width: '100%', height: '50px'}} />
            </div>
        </>
    )
        ;
}

export default Operations;
