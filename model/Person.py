"""
Person model class.
"""


from dataclasses import dataclass


@dataclass(frozen=True)
class Person(object):
    firstName: str
    lastName: str
    age: int
    weightInLbs: float =None
    jobType: str =None
