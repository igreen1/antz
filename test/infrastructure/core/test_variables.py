"""Test variable resolution module"""

from typing import Mapping

from antz.infrastructure.config.base import PrimitiveType
from antz.infrastructure.core.variables import (VARIABLE_PATTERN,
                                                _resolve_value,
                                                resolve_variables)


def test_regex_pattern() -> None:
    """Test the regex patter gets the correct group(s)"""

    expected_variables_names = {
        "%{a}": ["a"],
        "%{hello there!}": ["hello there!"],
        "%{oh123}": ["oh123"],
        "abc%": [],
        "abc%{hello}": ["hello"],
        "%{1}": ["1"],
        "%{hello}abc": ["hello"],
        "%{hello}a%{bye}": ["hello", "bye"],
    }

    for unparsed, expected in expected_variables_names.items():
        match = [x.lstrip("%{").rstrip("}") for x in VARIABLE_PATTERN.findall(unparsed)]
        assert match == expected


def test_simple_variable_replacement() -> None:
    """Test a simploe replacement of a variable string"""

    variables: Mapping[str, PrimitiveType] = {
        "a": 1,
        "b": 2,
        "c": "hello",
        "d": 0.123,
        "e": "true",
        "f": True,
        "g": False,
        "h": "faLsE",
    }

    expected_values = {
        "%{a}": 1,
        "hello%{b}": "hello2",
        "%{b}bye": "2bye",
        "abc%{c}def": "abchellodef",
        "%{b}": 2,
        "%{d}": 0.123,
        "hello%{d}": "hello0.123",
        "%{h}": False,
        "%{g}": False,
        "%{f}": True,
        "%{e}": True,
        "helo%{e}": "helotrue",
        "a%{f}b": "aTrueb",
    }

    for input_val, expected_val in expected_values.items():
        assert _resolve_value(input_val, variables=variables) == expected_val


def test_parameters_simple_replacement() -> None:
    """test a simple interpolation of the variables into parameters"""
    variables: Mapping[str, PrimitiveType] = {
        "a": 1,
        "b": 2,
        "c": "hello",
        "d": 0.123,
        "e": "true",
        "f": True,
        "g": False,
        "h": "faLsE",
    }

    expected_values = [
        ("%{a}", 1),
        ("hello%{b}", "hello2"),
        ("%{b}bye", "2bye"),
        ("abc%{c}def", "abchellodef"),
        ("%{b}", 2),
        ("%{d}", 0.123),
        ("hello%{d}", "hello0.123"),
        ("%{h}", False),
        ("%{g}", False),
        ("%{f}", True),
        ("%{e}", True),
        ("helo%{e}", "helotrue"),
        ("a%{f}b", "aTrueb"),
    ]

    input_parameters = {
        str(i): in_val for i, (in_val, _expected_out) in enumerate(expected_values)
    }
    output_parameters = {
        str(i): expected_out
        for i, (_in_val, expected_out) in enumerate(expected_values)
    }

    assert output_parameters == resolve_variables(input_parameters, variables=variables)


def test_parameter_variable_expressions() -> None:
    """Test that expressions are allowed in variables (basic math)"""

    variables: Mapping[str, PrimitiveType] = {
        "a": 1,
        "b": 2,
        "bb": 12,
        "c": "hello",
        "d": 0.123,
        "e": "true",
        "f": True,
        "g": False,
        "h": "faLsE",
    }

    expected_values = {
        ("%{a * b}", 2),
        ("%{a + b}", 3),
        ("%{a / b}", 1 / 2),
        ("%{a - b}", -1),
        ("%{a * b - bb}", -10),
        ("%{bb / b * b}", 3),
    }

    input_parameters = {
        str(i): in_val for i, (in_val, _expected_out) in enumerate(expected_values)
    }
    output_parameters = {
        str(i): expected_out
        for i, (_in_val, expected_out) in enumerate(expected_values)
    }

    assert output_parameters == resolve_variables(input_parameters, variables=variables)
