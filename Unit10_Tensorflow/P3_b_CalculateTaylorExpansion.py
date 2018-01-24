import numpy as np
import tensorflow as tf

tf.reset_default_graph

# Create inputs for sum(1/n!) for n = [0,1,2,...,12]
n = tf.constant(10000000.00, name = "a")
n = tf.cast(n,tf.float64)
a = tf.constant(1.00, name = "a") 
a = tf.cast(a,tf.float64)

# Create tensor operations for function
div = tf.divide(a,n)         # 1/n
add = tf.add(a,div)          # (1 + (1/n))
exp = tf.pow(add,n)          # (1 + (1/n))^n


init = tf.global_variables_initializer()  # initializes all

with tf.Session() as sess:
    sess.run(init)       # run init operation, initialize all
    print("Taylor series approximation for e: lim n->inf ((1+(1/n))^n) ")
    print("If n = %s" % sess.run(n))
    print("\n1/n equals: %s" % sess.run(div))
    print("\n(1+(1/n)) equals: %.5f" % sess.run(add))
    print("\ns(1+(1/n))^n equals: %s" % sess.run(exp))
    print("\nTherefore, approximation of e = %.6f" % sess.run(exp))

    file_writer = tf.summary.FileWriter("output2", sess.graph)
    file_writer.add_graph(sess.graph)
    file_writer.flush()
    file_writer.close()
    sess.close()
