import os
import sys

# Append the root directory to path so imports work out of the box
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api import app
