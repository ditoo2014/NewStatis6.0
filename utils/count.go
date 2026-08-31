package utils

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// precedence 定义受支持算术运算符的优先级。
var precedence = map[string]int{"+": 1, "-": 1, "*": 2, "/": 2}

// Tokenize 将公式拆分为数字、变量、括号和运算符标记。
func Tokenize(expression string) []string {
	var tokens []string
	var current strings.Builder
	for _, char := range expression {
		if unicode.IsDigit(char) || char == '.' || unicode.IsLetter(char) {
			current.WriteRune(char)
			continue
		}
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
		if !unicode.IsSpace(char) {
			tokens = append(tokens, string(char))
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// ReplaceVariables 按完整标记替换公式变量，避免误替换变量名的一部分。
func ReplaceVariables(expression string, variables map[string]string) (string, error) {
	tokens := Tokenize(expression)
	for i, token := range tokens {
		if value, ok := variables[token]; ok {
			tokens[i] = value
		}
	}
	return strings.Join(tokens, " "), nil
}

// InfixToPostfix 使用运算符栈将中缀表达式转换为后缀表达式。
func InfixToPostfix(expression string) ([]string, error) {
	var stack []string
	var output []string
	for _, token := range Tokenize(expression) {
		if len(token) == 0 {
			continue
		}
		if unicode.IsDigit(rune(token[0])) || token[0] == '.' {
			output = append(output, token)
		} else if token == "(" {
			stack = append(stack, token)
		} else if token == ")" {
			for len(stack) > 0 && stack[len(stack)-1] != "(" {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
			if len(stack) == 0 {
				return nil, fmt.Errorf("mismatched parentheses")
			}
			stack = stack[:len(stack)-1]
		} else {
			for len(stack) > 0 && precedence[stack[len(stack)-1]] >= precedence[token] {
				output = append(output, stack[len(stack)-1])
				stack = stack[:len(stack)-1]
			}
			stack = append(stack, token)
		}
	}
	for len(stack) > 0 {
		output = append(output, stack[len(stack)-1])
		stack = stack[:len(stack)-1]
	}
	return output, nil
}

// EvaluatePostfix 计算后缀表达式，并检查缺少操作数、未知运算符和除零错误。
func EvaluatePostfix(postfix []string) (float64, error) {
	var stack []float64
	for _, token := range postfix {
		if unicode.IsDigit(rune(token[0])) || token[0] == '.' {
			num, err := strconv.ParseFloat(token, 64)
			if err != nil {
				return 0, err
			}
			stack = append(stack, num)
			continue
		}
		if len(stack) < 2 {
			return 0, fmt.Errorf("invalid expression")
		}
		b := stack[len(stack)-1]
		a := stack[len(stack)-2]
		stack = stack[:len(stack)-2]
		switch token {
		case "+":
			stack = append(stack, a+b)
		case "-":
			stack = append(stack, a-b)
		case "*":
			stack = append(stack, a*b)
		case "/":
			if b == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			stack = append(stack, a/b)
		default:
			return 0, fmt.Errorf("unknown operator: %s", token)
		}
	}
	if len(stack) != 1 {
		return 0, fmt.Errorf("invalid expression")
	}
	return stack[0], nil
}

// ApplyFormula 将原始统计值代入变量 x 后计算配置公式；空公式直接返回原值。
func ApplyFormula(expression string, raw float64) (float64, error) {
	if expression == "" {
		return raw, nil
	}
	// 使用非科学计数法保留 float64 的有效小数，兼容当前公式分词规则。
	replaced, err := ReplaceVariables(expression, map[string]string{"x": strconv.FormatFloat(raw, 'f', -1, 64)})
	if err != nil {
		return 0, err
	}
	postfix, err := InfixToPostfix(replaced)
	if err != nil {
		return 0, err
	}
	return EvaluatePostfix(postfix)
}
