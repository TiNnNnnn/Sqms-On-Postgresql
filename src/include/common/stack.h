#pragma once 
#include "postgres.h"
#include "nodes/pg_list.h"
#include <stdio.h>

typedef struct Stack {
    List *items;
} Stack;

Stack *stack_init()
{
    Stack *stack = (Stack *) palloc(sizeof(Stack));
    stack->items = NIL;
    return stack;
}

void stack_push(Stack *stack, void *item)
{
    stack->items = lcons(item, stack->items);
}

void *stack_pop(Stack *stack)
{
    if (stack->items == NIL)
    {
        return NULL;  
    }
    ListCell *cell = list_head(stack->items);
    void *item = (void *) lfirst(cell);
    stack->items = list_delete_first(stack->items); 
    return item;
}

void *stack_peek(Stack *stack)
{
    if (stack->items == NIL)
    {
        return NULL;
    }

    ListCell *cell = list_head(stack->items);
    return (void *) lfirst(cell); 
}

bool stack_is_empty(Stack *stack)
{
    return stack->items == NIL;
}

void stack_clear(Stack *stack)
{
    list_free_deep(stack->items); 
    stack->items = NIL;
}