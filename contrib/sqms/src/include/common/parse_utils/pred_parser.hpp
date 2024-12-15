#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sstream>
#include <vector>
#include <string>
#include <stack>
#include <memory>
#include <iostream>

// 定义树节点结构
class ExprNode {
public:
    std::string value;
    ExprNode* left;
    ExprNode* right;

    // 构造函数
    ExprNode(const std::string& val) : value(val), left(nullptr), right(nullptr) {}
};

// 工具类：用于解析表达式并构建树
class ExprTreeBuilder {
public:
    // 构建表达式树
    ExprNode* buildTree(const std::string& expr) {
        std::stack<ExprNode*> nodeStack;
        std::stack<std::string> opStack;
        std::stringstream ss(expr);
        std::string token;

        while (ss >> token) {
            if (token == "(") {
                opStack.push(token);
            } else if (token == ")") {
                // 处理右括号
                while (!opStack.empty() && opStack.top() != "(") {
                    std::string op = opStack.top();
                    opStack.pop();
                    ExprNode* right = nodeStack.top();
                    nodeStack.pop();
                    ExprNode* left = nodeStack.top();
                    nodeStack.pop();
                    ExprNode* node = new ExprNode(op);
                    node->left = left;
                    node->right = right;
                    nodeStack.push(node);
                }
                opStack.pop(); // 弹出 '('
            } else if (token == "AND" || token == "OR") {
                opStack.push(token);
            } else {
                // 处理谓词
                nodeStack.push(new ExprNode(token));
            }
        }

        // 处理剩余的操作符
        while (!opStack.empty()) {
            std::string op = opStack.top();
            opStack.pop();
            ExprNode* right = nodeStack.top();
            nodeStack.pop();
            ExprNode* left = nodeStack.top();
            nodeStack.pop();
            ExprNode* node = new ExprNode(op);
            node->left = left;
            node->right = right;
            nodeStack.push(node);
        }

        return nodeStack.top();
    }

    // 打印树结构
    void printTree(ExprNode* node, int level = 0) {
        if (node != nullptr) {
            printTree(node->right, level + 1);
            std::cout << std::string(level * 4, ' ') << "-> " << node->value << std::endl;
            printTree(node->left, level + 1);
        }
    }
};