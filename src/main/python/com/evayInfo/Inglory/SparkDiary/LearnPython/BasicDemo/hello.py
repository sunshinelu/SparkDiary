# hello.py
# -*- coding: UTF-8 -*-

print "hello world"
print "你好"

if True:
    print "Ture"
else:
    print "False"

a = 'abc'
b = a
print b

print u'中文'
u'中'
print "======="
print ord('A')  # 字符串转ASCII编码
print chr(65)  # ASCII编码转字符串
print u'\u0041'  # A的Unicode编码
print u'A'
print "======="
print u'中'
print u'\u4e2d'  # Unicode编码
print u'中'.encode('utf-8')
print '\xe4\xb8\xad'  # UTF-8编码
print "======="
print u'中文'.encode('utf-8')
print '\xe4\xb8\xad\xe6\x96\x87'

print "把Unicode编码成GB2312"
print u'中文'.encode('gb2312')
print '\xd6\xd0\xce\xc4'

print "python list 操作"
classmates = ['Michael', 'Bob', 'Tracy']
print classmates
print len(classmates)
print classmates[0]
print classmates[-1]
print classmates[-2]
print classmates.append('Adam')
classmates.insert(1, 'Jack')
print classmates

print "list里面的元素的数据类型也可以不同"
L = ['Apple', 123, True]
print L
s = ['python', 'java', ['asp', 'php'], 'scheme']
print s
print len(s)
print s[2][1]

print "tuple 使用Demo"
classmates2 = ('Michael', 'Bob', 'Tracy')
t = ('a', 'b', ['A', 'B'])
print t
t[2][0] = "X"
t[2][1] = "Y"
print t
