import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

function SignUp() {
  const [username, setUsername] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [error, setError] = useState(null);
  const navigate = useNavigate();

  const handleSignUp = async (username, email, password) => {
    // Simulate API call (replace with real API call)
    return new Promise((resolve, reject) => {
      if(password.length < 8) {
        setTimeout(() => {
          reject({ error: 'The password need to be at least 8 character long' });
        }, 1000);
      }
      else{
        setTimeout(() => {
          resolve({ success: true });
        }, 1000);
      }
    });
  };
  const handleSubmit = async (event) => {
    event.preventDefault();
    setError(null);
    if (password !== confirmPassword) {
        setError('Passwords do not match');
        return;
    }
    try {
        const response = await handleSignUp(username, email, password);
        if (response.success) {
            console.log('Sign Up Successful');
            navigate('/signin');
        }
    } catch (err) {
        console.error('Sign Up Failed:', err.error);
        setError(err.error);
    }
  };


  return (
    <div className="sign-up-container">
      <h2>Sign Up</h2>
      {error && <div className="error-message">{error}</div>}
      <form onSubmit={handleSubmit}>
        <div className="form-group">
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
          <label htmlFor="email">Email:</label>
          <input
            type="email"
            id="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
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
        <div className="form-group">
          <label htmlFor="confirmPassword">Confirm Password:</label>
          <input
            type="password"
            id="confirmPassword"
            value={confirmPassword}
            onChange={(e) => setConfirmPassword(e.target.value)}
            required
          />
        </div>
        <button type="submit">Sign Up</button>
      </form>
    </div>
  );
}

export default SignUp;