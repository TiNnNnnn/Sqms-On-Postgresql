#include <iostream>
#include <stack>
#include <string>
#include <cctype>

using namespace std;

// 定义树节点结构
class ExprNode {
public:
    string value;
    ExprNode* left;
    ExprNode* right;

    // 构造函数
    ExprNode(const string& val) : value(val), left(nullptr), right(nullptr) {}
};

// 工具类：用于解析表达式并构建树
class ExprTreeBuilder {
public:
    // 构建表达式树
    ExprNode* buildTree(const string& expr) {
        stack<ExprNode*> nodeStack;
        stack<string> opStack;
        string token;
        size_t i = 0;

        while (i < expr.length()) {
            char ch = expr[i];

            if (isspace(ch)) {
                // 跳过空格
                i++;
                continue;
            }

            if (ch == '(') {
                opStack.push("(");
                i++;
            } else if (ch == ')') {
                // 处理右括号
                while (!opStack.empty() && opStack.top() != "(") {
                    string op = opStack.top();
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
                i++;
            } else if (ch == 'A' && expr.substr(i, 3) == "AND") {
                opStack.push("AND");
                i += 3;
            } else if (ch == 'O' && expr.substr(i, 2) == "OR") {
                opStack.push("OR");
                i += 2;
            } else {
                // 处理谓词（整个表达式，如 tt.a > 10 或 max(tt.c) > 10）
                size_t start = i;
                while (i < expr.length() && (isalnum(expr[i]) || expr[i] == '.' || expr[i] == '(' || expr[i] == ')' ||expr[i] == '>' || expr[i] == '<' || expr[i] == ' ')) {
                    i++;
                }
                token = expr.substr(start, i - start);
                nodeStack.push(new ExprNode(token));
            }
        }

        // 处理剩余的操作符
        while (!opStack.empty()) {
            string op = opStack.top();
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
            cout << string(level * 4, ' ') << "-> " << node->value << endl;
            printTree(node->left, level + 1);
        }
    }
};

int main() {
    // 表达式字符串
    //string expr = "(((tt.a > 10) AND (max(tt.c) > 10)) OR (tt.a < 20))";

    string expr = "(tt.a > 10)";
    // 创建工具类对象并构建树
    ExprTreeBuilder builder;
    ExprNode* tree = builder.buildTree(expr);

    // 打印树
    builder.printTree(tree);

    return 0;
}
