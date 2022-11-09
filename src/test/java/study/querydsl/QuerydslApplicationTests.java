package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Hello;
import study.querydsl.entity.QHello;

import javax.persistence.EntityManager;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@Transactional
@Commit
class QuerydslApplicationTests {

	@Autowired
	EntityManager em;

	@Test
	void contextLoads() {
		Hello hello = new Hello();
		System.out.println(hello.getId());

		em.persist(hello);

		JPAQueryFactory query = new JPAQueryFactory(em);
		QHello qHello = new QHello("h");

		Hello findHello = query
				.selectFrom(qHello)
				.fetchOne();

		assertThat(findHello).isEqualTo(hello);
		assertThat(findHello.getId()).isEqualTo(hello.getId());
		System.out.println(hello.getId());
		System.out.println(findHello.getId());
		System.out.println(hello.getClass());
		System.out.println(findHello.getClass());
	}

}
