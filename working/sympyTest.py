from sympy import sympify, Symbol

# expr_str = "1.3 + h*0.5 + d^2"

expr_str = " h**2 + d^2"

# sympy uses ** for power, so replace ^ with **
expr_str = expr_str.replace('^', '**')

# Parse expression safely
expr = sympify(expr_str)

# Example variable values:
vars_dict = {'h': 3, 'd': 4}

# Substitute variables
result = expr.subs(vars_dict)

# Evaluate to a number (float)
result_value = float(result)

print(result_value)  # e.g. 1.3 + 5*0.5 + 3**2 = 1.3 + 2.5 + 9 = 12.8
