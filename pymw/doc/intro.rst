=========================================
PyMW - Master Worker Computing for Python
=========================================
This is the documentation for PyMW (Python Master Worker). PyMW is a Python module for use in master-worker style parallel computation. PyMW provides a common API to multiple computation environments. This allows users to write a single Python program that can easily scale from a single computer to a worldwide computing grid.

The focus of PyMW is providing tools for simple, user-friendly parallel computing. Although high performance is a goal of PyMW, the main goal is simplicity.

------------------------------
Computations suitable for PyMW
------------------------------
**Parameter Sweep**

A parameter sweep is where the same computation is repeatedly performed using different inputs each time. This is often used to sweep through a range of parameters and find the relation between inputs and outputs.

**Monte Carlo**

The Monte Carlo method uses random numbers to account for uncertainty in a computation. The computation is run a large number of times and statistics are gathered. For example, the Monte Carlo method is used in physics simulations where particle decay is random.

**Genetic Algorithms/Optimization Techniques**

Genetic algorithms attempt to optimize a function with regards to a set of inputs. In this case, master-worker parallelization may work well in evaluating each possible solution.

-------------------------------------------
Types of computations not suitable for PyMW
-------------------------------------------
PyMW is not well suited for computations with frequent communication and sharing of data between workers.
