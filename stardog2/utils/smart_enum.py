from enum import Enum
from typing import List

from pydantic import validate_arguments


class SmartEnum(Enum):
    """A smart enumeration"""
    @classmethod
    @validate_arguments
    def has_value(cls, value: str) -> bool:
        """
        This function check whether the value exists in the enum.
        Args:
            value: The value you want to check for

        Returns:
            bool: if the value exists in the enum
        """
        return value in cls._value2member_map_

    @classmethod
    @validate_arguments
    def get(cls, value: str):
        """
        This function return the enum key from it value or throw an exception

        Args:
            value: the value you want the key for

        Raises:
            Value {value} is not valid, valid values: {valid}

        Return:
            Enum: if exists
        """

        for key in cls:
            if key.value == value:
                return key

        valid = cls.list()

        raise Exception(f"Value {value} is not valid, valid values: {valid} ")

    @classmethod
    @validate_arguments
    def list(cls) -> List[str]:
        """
        This will return a list of valid values for this enum

        Return:
            List: values
        """
        return [str(member.value) for member in cls]
