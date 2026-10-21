package de.kp.works.ignite;

import java.util.*;
import java.util.stream.Collectors;

import de.kp.works.ignite.query.*;

import de.kp.works.ignite.graph.*;

public final class IgniteTransform {
	public static final IgniteTransform MODULE$ = new IgniteTransform();

	public static List<IgniteResult> transformEdgeEntries(final List<IgniteEdgeEntry> entries) {
		List<IgniteResult> list = new LinkedList<IgniteResult>();

		Map<String, List<IgniteEdgeEntry>> groupByResult = entries.stream()
				.collect(Collectors.groupingBy(entry -> entry.id));

		groupByResult.forEach((k, values) -> {

			IgniteEdgeEntry head = values.get(0);

			/*
			 * Extract common fields
			 */

			String id = head.id;
			String label = head.label;

			IgniteResult igniteResult = new IgniteResult(id, label);

			String toId = head.toId;
			String toIdType = head.toIdType;

			String fromId = head.fromId;
			String fromIdType = head.fromIdType;

			igniteResult.setCreatedAt(head.createdAt);
			igniteResult.setUpdatedAt(head.updatedAt);

			/*
			 * Add common fields
			 */
			igniteResult.addColumn(IgniteConstants.TO_COL_NAME, toIdType, toId);

			igniteResult.addColumn(IgniteConstants.FROM_COL_NAME, fromIdType, fromId);
			/*
			 * Extract & add properties
			 */
			values.forEach(value -> {

				igniteResult.addColumn(value.propKey, value.propType, value.propValue);

			});

			list.add(igniteResult);

		});

		return list;
	}

	public static List<IgniteResult> transformVertexEntries(final List<IgniteVertexEntry> entries) {
		List<IgniteResult> list = new LinkedList<IgniteResult>();

		Map<String, List<IgniteVertexEntry>> groupByResult = entries.stream()
				.collect(Collectors.groupingBy(entry -> entry.id));

		groupByResult.forEach((k, values) -> {


			IgniteVertexEntry head = values.get(0);

			/*
			 * Extract common fields
			 */

			String id = head.id;

			short idType = head.idType;
			String label = head.label;

			IgniteResult igniteResult = new IgniteResult(id, label);
			igniteResult.setIdType(idType);
			igniteResult.setCreatedAt(head.createdAt);
			igniteResult.setUpdatedAt(head.updatedAt);

			/*
			 * Extract & add properties
			 */
			values.forEach(value -> {

				igniteResult.addColumn(value.propKey, value.propType, value.propValue);

			});

			list.add(igniteResult);

		});

		return list;
	}

	private IgniteTransform() {

	}

}
