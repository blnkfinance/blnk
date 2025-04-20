import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

function SignIn() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const handleSignIn = async (username, password) => {
    // Simulate API call (replace with real API call)
    return new Promise((resolve, reject) => {
        if (username === 'admin' && password === 'password') {
            setTimeout(() => {
                resolve({ success: true });
            }, 1000);
        } else {
            setTimeout(() => {
                reject({ error: 'Invalid username or password' });
            }, 1000);
        }
    });
  };
  const handleSubmit = async (event) => {
    event.preventDefault();
    setError(null);
    try {
        const response = await handleSignIn(username, password);
        if (response.success) {
            console.log('Sign In Successful');
            navigate('/');
            // Redirect to main console
        }
    } catch (err) {
        console.error('Sign In Failed:', err.error);
        setError(err.error);
    }
  };

  return (
    <div className="sign-in-container">
      <h2>Sign In</h2>
      <form onSubmit={handleSubmit}>
        <div className="form-group">
        {error && <div className="error-message">{error}</div>}
          <label htmlFor="username">Username:</label>
          <input
            type="text"
            id="username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            required
          />
        </div>
        <div className="form-group">
          <label htmlFor="password">Password:</label>
          <input
            type="password"
            id="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
        </div>
        <button type="submit">Sign In</button>
      </form>
    </div>
  );
}

export default SignIn;