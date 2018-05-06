package com.mudigal.two.service.impl;

import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.transaction.Transactional;
import javax.transaction.Transactional.TxType;

import org.apache.log4j.Logger;
import org.dozer.DozerBeanMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.mudigal.two.service.impl.NameValueServiceImpl;
import com.mudigal.two.dao.NameValueDao;
import com.mudigal.two.domain.NameValue;
import com.mudigal.two.model.AllNameValueTO;
import com.mudigal.two.model.NameValueTO;
import com.mudigal.two.service.NameValueService;

/**
 * 
 * @author Vijayendra Mudigal
 *
 */
@Service(value = "nameValueService")
@Transactional(value = TxType.REQUIRES_NEW)
public class NameValueServiceImpl implements NameValueService {

	private Logger logger = Logger.getLogger(NameValueServiceImpl.class);

	@PersistenceContext
	private EntityManager em;

	@Value("${spring.application.name}")
	private String applicationName;

	@Autowired
	private DozerBeanMapper dozerBeanMapper;

	@Autowired
	private NameValueDao nameValueDao;

	@Autowired(required = false)
	private ServiceTwoRabbitMessageProducer serviceTwoRabbitMessageProducer;

	@Override
	public NameValueTO updateNameValue(NameValueTO nameValueTO) {
		return updateNameValue(nameValueTO, false);
	}

	@Override
	public NameValueTO updateNameValue(NameValueTO nameValueTO, boolean fromRabbit) {
		try {
			em.merge(dozerBeanMapper.map(nameValueTO, NameValue.class));
			logger.info("Updated: " + nameValueTO);
			if (!fromRabbit && serviceTwoRabbitMessageProducer != null) {
				serviceTwoRabbitMessageProducer.sendMessageToQueue(nameValueTO);
			}
		} catch (Exception ex) {
			return new NameValueTO();
		}
		return nameValueTO;
	}

	@Override
	public AllNameValueTO getAllNameValues(String name) {
		Iterable<NameValue> nameValues = nameValueDao.findAll();
		AllNameValueTO allNameValueTO = new AllNameValueTO();
		for (NameValue nameValue : nameValues) {
			if (nameValue.getName().equals(name)) {
				allNameValueTO.setOriginalName(nameValue.getName());
				allNameValueTO.setOriginalValue(nameValue.getValue());
			} else {
				allNameValueTO.getRemainingNameValuePair().put(nameValue.getName(), nameValue.getValue());
			}
		}
		return allNameValueTO;
	}

	@Override
	@Scheduled(fixedDelay = 60000)
	public NameValueTO generateUUID() {
		NameValueTO nameValueTO = new NameValueTO();
		nameValueTO.setName(applicationName);
		nameValueTO.setValue(UUID.randomUUID().toString());
		return updateNameValue(nameValueTO);
	}

}
