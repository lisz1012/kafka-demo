package com.lisz.kafka;

public class Person {
	volatile int age = 10;

	public void m() {
		age = 100;
	}

}

/*
 0 aload_0
 1 invokespecial #1 <java/lang/Object.<init>>
 4 aload_0
 5 bipush 10
 7 putfield #2 <com/lisz/kafka/Person.age>
10 return
 */


/*
 0 aload_0
 1 invokespecial #1 <java/lang/Object.<init>>
 4 aload_0
 5 bipush 10
 7 putfield #2 <com/lisz/kafka/Person.age>
10 return
 */