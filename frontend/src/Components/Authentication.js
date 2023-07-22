import React, { useState } from 'react';
import axios from 'axios';

function Auth({authMessage, setAuthMessage}) {

    const authenticate = async () => {
        try {
            const response = await axios.post(
                'http://35.221.21.180:8081/v1/auth/token/generate',
                {
                    key: "cassandra",
                    secret: "cassandra"
                },
                {
                    headers: {
                        'accept': 'application/json',
                        'Content-Type': 'application/json',
                    }
                }
            );
            // Convert the response data to a string if it's an object
            if(typeof response.data === 'object') {
                setAuthMessage(JSON.stringify(response.data, null, 2));
            } else {
                setAuthMessage(response.data);
            }
        } catch (error) {
            console.log(error);
        }
    };

    return (
        <div>
            <h2> Step1: Get Authentication Token</h2>
            <textarea readOnly value={authMessage} style={{width: '100%', height: '50px'}} />
            <button onClick={authenticate}>Authenticate</button>
        </div>
    );
}

export default Auth;
