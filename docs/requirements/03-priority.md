Focus: Add priority scheduling

## Job model
* Priority level (higher = more urgent)

use 3 streams with high default and low priority tasks.
Your worker uses XREADGROUP to read from both streams simultaneously.
add support for api
