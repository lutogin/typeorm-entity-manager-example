import { size, get } from 'lodash';
import { Logger } from 'winston';
import { Job, Queue } from 'bull';
import * as Sentry from '@sentry/node';
import { setQueues } from 'bull-board';
import { EntityManager } from 'typeorm';
import { InjectEntityManager } from '@nestjs/typeorm';
import { Inject } from '@nestjs/common';
import { InjectQueue, OnQueueActive, OnQueueFailed, OnQueueCompleted, Process, Processor } from '@nestjs/bull';

import { RefreshCompanySnapshotToken, ITEMS_PER_REQUEST, COMPLETED_JOB } from '../../metrics/jobs/constants';
import { RefreshSnapshot } from './refresh-snapshot.interface';

import { SnapshotType } from '../snapshot.type';
import { AmoCrmClient } from '../../../amocrm/amocrm.client';
import { ConnectionRepository } from '../../connections/connection.repository';
import { SnapshotService } from '../snapshot.service';
import { Company } from '../entities/company.entity';

@Processor(RefreshCompanySnapshotToken)
export class RefreshCompanySnapshotHandler {
  constructor(
    @Inject('winston')
    private readonly logger: Logger,
    @InjectQueue(RefreshCompanySnapshotToken)
    private readonly refreshQueue: Queue<RefreshSnapshot>,
    @InjectEntityManager()
    private entityManager: EntityManager,
    private readonly snapshotService: SnapshotService,
    private readonly connectionRepository: ConnectionRepository,
    private readonly amoClient: AmoCrmClient,
  ) {
    setQueues(this.refreshQueue);
  }

  @Process()
  async handle(job: Job<RefreshSnapshot>): Promise<void> {
    let total = 0;
    let offset = 0;
    const itemsToDb = [];

    const { connectionId, companyId } = job.data;
    const from = this.snapshotService.getSnapshotDateStart(job.data.from);

    const { credentials, referer } = await this.connectionRepository.findOne({
      id: connectionId, companyId,
    });

    try {
      do {
        const { _embedded: result } = await this.amoClient.getCompanies(
          credentials, referer, from, offset,
        );

        const companies = get(result, 'items', []);

        for (const company of companies) {
          itemsToDb.push(
            await this.entityManager.create(
              Company,
              this.snapshotService.buildCompany(company, connectionId, companyId),
            ),
          );
        }

        total = size(companies);
        offset += size(companies);

        this.logger.info('Pushed companies to database', { total, referer });
      } while (total === ITEMS_PER_REQUEST);

      if (itemsToDb.length) {
        await this.entityManager.save(Company, itemsToDb);
      }

      await this.entityManager.query('refresh materialized view companies_view');

      await job.progress(COMPLETED_JOB);
    } catch (e) {
      throw get(e, 'response.data', e);
    }
  }

  @OnQueueActive()
  async handleRefreshStarted(job: Job<RefreshSnapshot>): Promise<void> {
    const { connectionId, companyId } = job.data;
    await this.snapshotService.refreshProcessStarted(companyId, connectionId, SnapshotType.COMPANY);
  }

  @OnQueueCompleted()
  async handleRefreshCompleted(job: Job<RefreshSnapshot>): Promise<void> {
    const { connectionId, companyId } = job.data;
    await this.snapshotService.refreshProcessCompleted(companyId, connectionId, SnapshotType.COMPANY);
  }

  @OnQueueFailed()
  async handleError(job: Job<RefreshSnapshot>, error: Error) {
    await this.snapshotService.refreshProcessCompleted(
      job.data.companyId,
      job.data.connectionId,
      SnapshotType.COMPANY,
    );

    Sentry.withScope((scope) => {
      scope.setTag('job.id', job.id.toString());
      scope.setTag('job.queue', job.queue.name);
      Sentry.captureException(error);
    });
  }
}
