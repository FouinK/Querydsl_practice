package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;

import javax.persistence.EntityManager;
import java.util.List;

@SpringBootTest
@Transactional
public class PresentationTest {

    @Autowired
    EntityManager em;
    JPAQueryFactory queryFactory;


    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);

        Member member = new Member("username", 10);
        em.persist(member);
    }

    @Test
    @DisplayName("기본 사용법")
    public void findAll() {
        QMember member = QMember.member;

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.username.eq("username"))
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }

    }
}
