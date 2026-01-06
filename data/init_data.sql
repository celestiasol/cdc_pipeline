-- Drop existing tables if they exist
DROP TABLE IF EXISTS public.users;
DROP TABLE IF EXISTS public.orders;

-- Create tables
CREATE TABLE public.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE public.orders (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES public.users(id),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    updated_at TIMESTAMP DEFAULT now()
);

-- Insert dummy data into users
INSERT INTO public.users (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com'),
('Charlie', 'charlie@example.com');

-- Insert dummy data into orders
INSERT INTO public.orders (user_id, amount, status) VALUES
(1, 120.50, 'completed'),
(2, 75.00, 'pending'),
(3, 250.00, 'completed');
